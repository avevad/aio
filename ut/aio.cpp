#include <gtest/gtest.h>

#include "AIOxx/fd.hpp"
#include "AIOxx/scheduler.hpp"

#include <array>
#include <chrono>
#include <span>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

using namespace AIO;
using namespace std::chrono_literals;

namespace {

struct RawFD {
  int fd = -1;

  RawFD() = default;
  explicit RawFD(int fd) : fd(fd) {
  }

  RawFD(const RawFD &) = delete;
  RawFD &operator=(const RawFD &) = delete;

  RawFD(RawFD &&other) noexcept : fd(other.fd) {
    other.fd = -1;
  }

  RawFD &operator=(RawFD &&other) noexcept {
    if (this == &other)
      return *this;
    reset();
    fd = other.fd;
    other.fd = -1;
    return *this;
  }

  ~RawFD() {
    reset();
  }

  void reset() {
    if (fd != -1) {
      ::close(fd);
      fd = -1;
    }
  }

  int release() {
    int out = fd;
    fd = -1;
    return out;
  }
};

struct Pipe {
  RawFD r, w;
};

Pipe make_pipe() {
  int fds[2] = {-1, -1};
  int res = ::pipe(fds);
  AIOXX_ASSUME(res == 0);
  return {.r = RawFD(fds[0]), .w = RawFD(fds[1])};
}

StreamFD wrap_stream(BasicScheduler *sched, RawFD &&fd) {
  return StreamFD(FD::steal_from_system(sched->io(), fd.release()));
}

StreamFD::OctetStream bytes(std::string_view s) {
  return {reinterpret_cast<const StreamFD::Octet *>(s.data()), s.size()};
}

std::string as_string(std::span<const StreamFD::Octet> s) {
  return {reinterpret_cast<const char *>(s.data()), s.size()};
}

} // namespace

TEST(AIO, Fiber) {
  run_in_new([](BasicScheduler *sched) {
    std::vector<int> seq;

    auto f1 = sched->fiber([&] {
      seq.push_back(1);
      sched->yield();
      seq.push_back(3);
    });
    auto f2 = sched->fiber([&] { seq.push_back(2); });

    sched->await(std::move(f1));
    sched->await(std::move(f2));
    EXPECT_EQ(seq, (std::vector{1, 2, 3}));

    EXPECT_EQ(sched->io().scheduler(), sched);
  });
}

TEST(AIO, Timers) {
  run_in_new([](BasicScheduler *sched) {
    auto past = std::chrono::steady_clock::now() - 1ms;
    sched->await(sched->deadline(past));

    auto t0 = std::chrono::steady_clock::now();
    sched->await(sched->timeout(2ms));
    EXPECT_GE(std::chrono::steady_clock::now() - t0, 0ms);

    auto add1 = sched->async([](int x) { return x + 1; });
    EXPECT_EQ(sched->await(add1(4)), 5);
  });
}

TEST(AIO, PipeRead) {
  run_in_new([](BasicScheduler *sched) {
    auto p = make_pipe();
    auto r = wrap_stream(sched, std::move(p.r));
    auto w = wrap_stream(sched, std::move(p.w));

    const std::string msg = "hello";
    auto writer = sched->fiber([&] {
      sched->await(sched->timeout(2ms));
      EXPECT_EQ(w.write(bytes(msg)), static_cast<StreamFD::StreamSize>(msg.size()));
    });

    std::vector<StreamFD::Octet> buf(msg.size());
    auto n = r.read({buf.data(), buf.size()});
    EXPECT_EQ(n, static_cast<StreamFD::StreamSize>(msg.size()));
    EXPECT_EQ(as_string({buf.data(), static_cast<size_t>(n)}), msg);

    sched->await(std::move(writer));
    std::move(r).close();
    std::move(w).close();
  });
}

TEST(AIO, ReadyStored) {
  run_in_new([](BasicScheduler *sched) {
    auto p = make_pipe();
    auto r = wrap_stream(sched, std::move(p.r));
    auto w = wrap_stream(sched, std::move(p.w));

    (void) w.write(bytes("x"));
    sched->yield();
    sched->await(r.ready(FD::IN));

    sched->yield();
    sched->await(w.ready(FD::OUT));

    int sys_w = std::move(w).release_to_system();
    ::close(sys_w);
    std::move(r).close();
  });
}

TEST(AIO, PipeWrite) {
  run_in_new([](BasicScheduler *sched) {
    auto p = make_pipe();
    auto r = wrap_stream(sched, std::move(p.r));
    auto w = wrap_stream(sched, std::move(p.w));

    std::vector<StreamFD::Octet> chunk(64 * 1024);
    size_t filled = 0;
    bool blocked = false;
    for (int i = 0; i < 1024; ++i) {
      auto n = w.try_write({chunk.data(), chunk.size()});
      if (!n.has_value()) {
        blocked = true;
        break;
      }
      filled += static_cast<size_t>(*n);
    }
    ASSERT_TRUE(blocked);
    ASSERT_GT(filled, 0u);

    const std::string msg = "ok";
    const size_t need = filled + msg.size();
    std::string tail;

    auto reader = sched->fiber([&] {
      sched->await(sched->timeout(2ms));

      size_t got = 0;
      std::array<StreamFD::Octet, 8192> buf{};
      while (got < need) {
        auto n = r.try_read({buf.data(), buf.size()});
        if (!n.has_value()) {
          sched->await(r.ready(FD::IN));
          continue;
        }
        got += static_cast<size_t>(*n);
        tail.append(reinterpret_cast<const char *>(buf.data()), static_cast<size_t>(*n));
        if (tail.size() > msg.size())
          tail.erase(0, tail.size() - msg.size());
      }
    });

    EXPECT_EQ(w.write(bytes(msg)), static_cast<StreamFD::StreamSize>(msg.size()));
    sched->await(std::move(reader));
    EXPECT_EQ(tail, msg);

    std::move(r).close();
    std::move(w).close();
  });
}

TEST(AIO, Hangup) {
  run_in_new([](BasicScheduler *sched) {
    auto p = make_pipe();
    auto r = wrap_stream(sched, std::move(p.r));
    auto w = wrap_stream(sched, std::move(p.w));

    auto fin = r.ready(FD::IN);
    auto fout = r.ready(FD::OUT);
    std::move(w).close();

    sched->await(std::move(fin));
    sched->await(std::move(fout));

    std::move(r).close();
  });
}
