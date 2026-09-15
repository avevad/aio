#include "AIOxx/scheduler.hpp"

#include "AIOxx/fd.hpp"

namespace AIO {

BasicScheduler *BasicScheduler::IO::scheduler() {
  return &parent;
}

void BasicScheduler::IO::watch(IOQueue::Handle *handle) {
  parent.pending_io_tasks.add(handle);
}

void BasicScheduler::IO::forget(IOQueue::Handle *handle) {
  parent.pending_io_tasks.erase(handle);
}

BasicScheduler::IO::IO(BasicScheduler &parent) : parent(parent) {
}

bool BasicScheduler::Timer::operator<(const Timer &timer) const {
  return when < timer.when;
}

BasicScheduler::BasicScheduler() {
}

void BasicScheduler::yield() {
  auto [promise, future] = AIO::make_contract<void>();
  std::move(promise).fulfill();
  await(std::move(future));
}

BasicScheduler::IO &BasicScheduler::io() {
  return fd_io;
}

void BasicScheduler::resume_fiber(Fiber fiber) {
  AIOXX_ASSUME(current_fiber == nullptr);
  current_fiber = std::move(fiber);
  current_fiber->coro.resume();
  AIOXX_ASSUME(current_fiber == nullptr);
}

const StreamFD &BasicScheduler::std_in() {
  if (!maybe_std_in) {
    maybe_std_in = std::make_unique<StreamFD>(FD::steal_from_system(fd_io, 0));
  }
  return *maybe_std_in;
}

const StreamFD &BasicScheduler::std_out() {
  if (!maybe_std_out) {
    maybe_std_out = std::make_unique<StreamFD>(FD::steal_from_system(fd_io, 1));
  }
  return *maybe_std_out;
}

const StreamFD &BasicScheduler::std_err() {
  if (!maybe_std_err) {
    maybe_std_err = std::make_unique<StreamFD>(FD::steal_from_system(fd_io, 2));
  }
  return *maybe_std_err;
}

Future<void> BasicScheduler::deadline(const std::chrono::time_point<std::chrono::steady_clock> &time) {
  auto [promise, future] = make_contract<void>();
  auto task = [promise = std::move(promise)] mutable { std::move(promise).fulfill(); };
  pending_timed_tasks.emplace(time, std::move(task));
  return std::move(future);
}

void BasicScheduler::run() {
  try {
    while (true) {
      { // Check pending tasks for immediate availability -- this is crucial for fairness guarantee.
        auto now = std::chrono::steady_clock::now();
        while (!pending_timed_tasks.empty() && pending_timed_tasks.begin()->when <= now) {
          Task task = std::move(pending_timed_tasks.extract(pending_timed_tasks.begin()).value().what);
          available_tasks.push(std::move(task));
        }
        if (auto task = pending_io_tasks.poll(now))
          available_tasks.push(std::move(*task));
      }

      // Execute first available task, if any.
      if (!available_tasks.empty()) {
        Task task = std::move(available_tasks.front());
        available_tasks.pop();
        task();
        continue;
      }

      // Check whether the scheduler is done completely.
      if (pending_timed_tasks.empty() && pending_io_tasks.empty()) {
        break;
      }

      { // Otherwise block on I/O and wait until anything happens...
        auto deadline = pending_timed_tasks.empty() ? std::nullopt : std::optional{pending_timed_tasks.begin()->when};
        if (auto task = pending_io_tasks.poll(deadline))
          available_tasks.push(std::move(*task));
      }
    }
  } catch (...) {
    panic("unexpected exception in scheduler", std::current_exception());
  }
}

BasicScheduler::~BasicScheduler() {
  AIOXX_ASSUME(current_fiber == nullptr);
  if (maybe_std_in) {
    (void) std::move(*maybe_std_in).release_to_system();
    maybe_std_in.reset();
  }
  if (maybe_std_out) {
    (void) std::move(*maybe_std_out).release_to_system();
    maybe_std_out.reset();
  }
  if (maybe_std_err) {
    (void) std::move(*maybe_std_err).release_to_system();
    maybe_std_err.reset();
  }
}

} // namespace AIO
