#pragma once

#include "coroutine.hpp"
#include "future.hpp"
#include "io.hpp"

#include <chrono>
#include <queue>
#include <set>

namespace AIO {
class StreamFD;
class BasicScheduler {
public:
  class IO;

  BasicScheduler();

  BasicScheduler(const BasicScheduler &) = delete;
  BasicScheduler(BasicScheduler &&other) = delete;

  BasicScheduler &operator=(const BasicScheduler &) = delete;
  BasicScheduler &operator=(BasicScheduler &&other) = delete;

  template<typename Functor, typename... Args>
  Future<std::invoke_result_t<Functor, Args...>> fiber(Functor &&fun, Args &&...args);

  template<typename Rep, typename Period>
  Future<void> timeout(const std::chrono::duration<Rep, Period> &duration);

  Future<void> deadline(const std::chrono::time_point<std::chrono::steady_clock> &time);

  template<typename Functor>
  auto async(Functor &&fun);

  template<typename Res>
  Res await(Future<Res> future);

  void yield();

  IO &io();

  const StreamFD &std_in();
  const StreamFD &std_out();
  const StreamFD &std_err();

  ~BasicScheduler();

  class IO {
  public:
    IO(const IO &) = delete;
    IO(IO &&) noexcept = delete;
    IO &operator=(const IO &) = delete;
    IO &operator=(IO &&) = delete;

    [[nodiscard]] BasicScheduler *scheduler();

    void watch(IOQueue::Handle *handle);
    void forget(IOQueue::Handle *handle);

  private:
    friend BasicScheduler;
    explicit IO(BasicScheduler &parent);

    BasicScheduler &parent;
  };

private:
  struct BaseFiber {
    Coroutine<void()> coro;
    template<typename... CoroArgs>
    explicit BaseFiber(CoroArgs &&...args);

    virtual bool is_cancelled() = 0;
    virtual ~BaseFiber() = default;
  };

  template<typename Res>
  struct TypedFiber : BaseFiber {
    Promise<Res> promise;
    template<typename... CoroArgs>
    explicit TypedFiber(Promise<Res> promise, CoroArgs &&...args);

    bool is_cancelled() override;
  };

  using Fiber = std::unique_ptr<BaseFiber>;
  using Task = std::move_only_function<void()>;
  struct Timer {
    bool operator<(const Timer &timer) const;

    std::chrono::time_point<std::chrono::steady_clock> when;
    Task what;
  };

  // TODO: separate scheduler interface from its management function(s)
  template<typename MainFunctor>
  friend void run_in_new(MainFunctor &&main);
  void run();

  void resume_fiber(Fiber fiber);

  std::queue<Task> available_tasks = {};
  std::multiset<Timer> pending_timed_tasks = {};
  IOQueue pending_io_tasks = {};

  Fiber current_fiber = nullptr;

  IO fd_io{*this};
  std::unique_ptr<StreamFD> maybe_std_in;
  std::unique_ptr<StreamFD> maybe_std_out;
  std::unique_ptr<StreamFD> maybe_std_err;
};
} // namespace AIO


// --------------------------------------------------
// -------------- TEMPLATE DEFINITIONS --------------
// --------------------------------------------------


namespace AIO {

template<typename Functor, typename... Args>
Future<std::invoke_result_t<Functor, Args...>> BasicScheduler::fiber(Functor &&fun, Args &&...args) {
  using Res = std::invoke_result_t<Functor, Args...>;
  auto [promise, future] = make_contract<Res>();
  auto fiber = std::make_unique<TypedFiber<Res>>(
    std::move(promise),
    [this, fun = std::forward<Functor>(fun),
     args = std::tuple<std::decay_t<Args>...>(std::forward<Args>(args)...)] mutable {
      auto invoker = [&]<typename... A>(A &&...a) mutable {
        return std::invoke(std::move(fun), std::forward<A>(a)...);
      };
      auto &promise = static_cast<TypedFiber<Res> *>(current_fiber.get())->promise;
      try {
        if constexpr (!std::is_void_v<Res>) {
          std::move(promise).fulfill(std::apply(invoker, std::move(args)));
        } else {
          std::apply(invoker, std::move(args));
          std::move(promise).fulfill();
        }
        /*
      TODO: with proper implementation of Future cancelling this should look like this:
      } catch (const _impl::CoroutineKiller &) {*/
      } catch (...) {
        std::move(promise).fail_any(std::current_exception());
      }
      AIOXX_ASSUME(current_fiber != nullptr);

      // It is fiber's responsibility to deschedule itself, but we cannot destroy it right here as it is still alive.
      // Instead, we deschedule the fiber and schedule a task to dispose of the fiber after its completion.
      available_tasks.push([fiber = std::move(current_fiber)] mutable { fiber.reset(); });
    }
  );
  available_tasks.push([this, fiber = std::move(fiber)] mutable { resume_fiber(std::move(fiber)); });
  return std::move(future);
}

template<typename Functor>
auto BasicScheduler::async(Functor &&fun) {
  return [this, fun = std::forward<Functor>(fun)]<typename... Args>(Args &&...args) {
    return this->fiber(fun, std::forward<Args>(args)...);
  };
}

template<typename Res>
[[nodiscard]] Res BasicScheduler::await(Future<Res> future) {
  AIOXX_ASSUME(current_fiber != nullptr);

  // See comments below.
  auto *fiber = current_fiber.get();

  Expected<Res> expected = std::unexpected<std::exception_ptr>(nullptr);
  std::move(future)
    .map_expected(
      [this, fiber = std::move(current_fiber),
       &expected](std::expected<Res, std::exception_ptr> expected1) mutable -> std::expected<void, std::exception_ptr> {
        expected = std::move(expected1);
        available_tasks.push([this, fiber = std::move(fiber)] mutable { resume_fiber(std::move(fiber)); });
        return {};
      }
    )
    .detach();

  // Future consumer will live until executed once and the fiber will be held at least to this point.
  // Then the fiber will be moved into queue and by that means will live until resumed.
  // However, the consumer can be executed immediately, so TODO -- examine fiber lifetime more carefully at this moment:
  fiber->coro.yield();

  if (!expected.has_value())
    std::rethrow_exception(expected.error());

  if constexpr (!std::is_void_v<Res>)
    return std::move(*expected);
  else
    return;
}

template<typename... CoroArgs>
BasicScheduler::BaseFiber::BaseFiber(CoroArgs &&...args) : coro(std::forward<CoroArgs>(args)...) {
}

template<typename Res>
template<typename... CoroArgs>
BasicScheduler::TypedFiber<Res>::TypedFiber(Promise<Res> promise, CoroArgs &&...args)
    : BaseFiber(std::forward<CoroArgs>(args)...), promise(std::move(promise)) {
}

template<typename Res>
bool BasicScheduler::TypedFiber<Res>::is_cancelled() {
  return promise.is_free();
}

template<typename Rep, typename Period>
Future<void> BasicScheduler::timeout(const std::chrono::duration<Rep, Period> &duration) {
  return deadline(
    std::chrono::steady_clock::now() +
    std::chrono::duration_cast<
      std::chrono::steady_clock::duration, std::chrono::steady_clock::rep, std::chrono::steady_clock::period>(duration)
  );
}

template<typename MainFunctor>
void run_in_new(MainFunctor &&main) {
  auto sched = std::make_unique<BasicScheduler>();

  auto run_main = sched->async([sched = sched.get(), main = std::forward<MainFunctor>(main)] { main(sched); });
  auto catch_all = sched->async([](auto err) { panic("unhandled exception", err); });
  auto completed = run_main().except_any(catch_all);

  sched->run();
  if (!completed.is_fulfilled()) {
    panic("deadlock detected");
  }
  std::move(completed).detach();
}

} // namespace AIO
