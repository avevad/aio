#pragma once

#include <chrono>
#include <cstdint>
#include <functional>

#include "future.hpp"

namespace AIO {
using SystemFD = int;

class IOQueue {
public:
  using Task = std::move_only_function<void()>;

  class [[nodiscard]] Handle {
  public:
    Handle(SystemFD fd);
    Handle(Handle &&other) noexcept;
    Handle &operator=(Handle &&other) noexcept;

    Handle(const Handle &) = delete;
    Handle &operator=(const Handle &) = delete;

    Future<void> ready_in();
    Future<void> ready_out();

    ~Handle();

  private:
    friend IOQueue;

    SystemFD fd;
    IOQueue *queue = nullptr;

    // TODO: thread-safety.
    std::optional<Promise<void>> prom_in = std::nullopt, prom_out = std::nullopt;

    // TODO: platform independence.
    uint32_t epoll_events = 0;
    // Intrusive list of scheduled IOQueue updates
    Handle *next_update = nullptr;
    Handle **prev_ptr = nullptr;
  };

  IOQueue();
  IOQueue(const IOQueue &) = delete;
  IOQueue(IOQueue &&) noexcept = delete;
  IOQueue &operator=(const IOQueue &) = delete;
  IOQueue &operator=(IOQueue &&) noexcept = delete;

  void add(Handle *handle);
  void erase(Handle *handle);
  [[nodiscard]] std::optional<Task> poll(std::optional<std::chrono::time_point<std::chrono::steady_clock>> deadline);

  [[nodiscard]] bool empty() const;

  ~IOQueue();

private:
  void schedule_update(Handle *handle);
  void remove_update(Handle *handle);

  int ep_fd;
  size_t pending_consumers = 0;
  Handle *updates = nullptr;
};
} // namespace AIO
