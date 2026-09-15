#include "AIOxx/io.hpp"

#include "AIOxx/fd.hpp"
#include "AIOxx/util.hpp"

#include <cerrno>
#include <cstring>
#include <sys/epoll.h>
#include <unistd.h>

namespace AIO {

IOQueue::Handle::Handle(SystemFD fd) : fd(fd) {
}

IOQueue::Handle::Handle(Handle &&other) noexcept
    : fd(other.fd), prom_in(std::move(other.prom_in)), prom_out(std::move(other.prom_out)) {
  AIOXX_ASSUME(other.queue == nullptr);

  other.fd = -1;
  other.queue = nullptr;
  other.prom_in.reset();
  other.prom_out.reset();
}

IOQueue::Handle &IOQueue::Handle::operator=(Handle &&other) noexcept {
  AIOXX_ASSUME(queue == nullptr);
  AIOXX_ASSUME(other.queue == nullptr);

  if (this == &other)
    return *this;

  fd = other.fd;
  queue = other.queue;
  prom_in = std::move(other.prom_in);
  prom_out = std::move(other.prom_out);

  other.fd = -1;
  other.queue = nullptr;
  other.prom_in.reset();
  other.prom_out.reset();

  return *this;
}

Future<void> IOQueue::Handle::ready_in() {
  AIOXX_ASSUME(!prom_in.has_value());
  auto [promise, future] = make_contract<void>();
  prom_in = std::move(promise);
  if (queue) {
    ++queue->pending_consumers;
    queue->schedule_update(this);
  }
  return std::move(future);
}

Future<void> IOQueue::Handle::ready_out() {
  AIOXX_ASSUME(!prom_out.has_value());
  auto [promise, future] = make_contract<void>();
  prom_out = std::move(promise);
  if (queue) {
    ++queue->pending_consumers;
    queue->schedule_update(this);
  }
  return std::move(future);
}

IOQueue::Handle::~Handle() {
  AIOXX_ASSUME(queue == nullptr);
}

IOQueue::IOQueue() {
  if ((ep_fd = epoll_create1(EPOLL_CLOEXEC)) < 0)
    panic(strerror(errno));
}

void IOQueue::add(Handle *handle) {
  AIOXX_ASSUME(handle->queue == nullptr);
  handle->queue = this;
  pending_consumers += handle->prom_in.has_value() + handle->prom_out.has_value();
  if (handle->prom_in || handle->prom_out)
    schedule_update(handle);
}

void IOQueue::erase(Handle *handle) {
  AIOXX_ASSUME(handle->queue == this);
  remove_update(handle);
  if (handle->epoll_events != 0 && epoll_ctl(ep_fd, EPOLL_CTL_DEL, handle->fd, nullptr) == -1)
    panic(strerror(errno));
  handle->epoll_events = 0;
  pending_consumers -= handle->prom_in.has_value() + handle->prom_out.has_value();
  handle->queue = nullptr;
}

void IOQueue::schedule_update(Handle *handle) {
  // Check whether the update is still needed (has epoll mismatch)
  uint32_t events = (handle->prom_in ? EPOLLIN : 0) | (handle->prom_out ? EPOLLOUT : 0);
  if ((events ^ handle->epoll_events) == 0) {
    remove_update(handle);
    return;
  }
  // If the update is already queued, skip
  if (handle->prev_ptr)
    return;
  // Update list links
  handle->next_update = updates;
  handle->prev_ptr = &updates;
  if (updates)
    updates->prev_ptr = &handle->next_update;
  updates = handle;
}

void IOQueue::remove_update(Handle *handle) {
  // Id the update isn't even queued, skip
  if (!handle->prev_ptr)
    return;
  // Update list links
  *handle->prev_ptr = handle->next_update;
  if (handle->next_update)
    handle->next_update->prev_ptr = handle->prev_ptr;
  handle->prev_ptr = nullptr;
  handle->next_update = nullptr;
}

std::optional<IOQueue::Task> IOQueue::poll(std::optional<std::chrono::time_point<std::chrono::steady_clock>> deadline) {
  // Apply all queued fd updates
  while (updates) {
    auto *handle = updates;
    remove_update(handle);
    uint32_t events = (handle->prom_in ? EPOLLIN : 0) | (handle->prom_out ? EPOLLOUT : 0);
    int op = events == 0 ? EPOLL_CTL_DEL : handle->epoll_events == 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
    epoll_event ep_evt{.events = events, .data = {.ptr = handle}};
    if (epoll_ctl(ep_fd, op, handle->fd, events ? &ep_evt : nullptr) == -1)
      panic(strerror(errno));
    handle->epoll_events = events;
  }
  // Select one event from epoll
  epoll_event ep_evt{};
  int result;
  while (true) {
    // Recompute the remaining timeout after interruptions.
    int timeout_num = -1;
    if (deadline.has_value()) {
      auto now = std::chrono::steady_clock::now();
      auto timeout = *deadline < now ? std::chrono::steady_clock::duration{0} : *deadline - now;
      // TODO: clamp to INT_MAX and round positive fractional milliseconds up.
      timeout_num = std::chrono::duration_cast<std::chrono::duration<int, std::milli>>(timeout).count();
    }
    result = epoll_wait(ep_fd, &ep_evt, 1, timeout_num);
    if (result >= 0)
      break;
    if (errno != EINTR)
      panic(strerror(errno));
    if (deadline && std::chrono::steady_clock::now() >= *deadline)
      return std::nullopt;
  }
  if (result) {
    // Multiplex the event to corresponding consumer promise(s)
    auto *handle = static_cast<Handle *>(ep_evt.data.ptr);
    std::optional<Promise<void>> in = std::nullopt, out = std::nullopt;
    if (handle->prom_in && (ep_evt.events & (EPOLLIN | EPOLLERR | EPOLLHUP))) {
      in = std::move(handle->prom_in);
      handle->prom_in.reset();
      --pending_consumers;
    }
    if (handle->prom_out && (ep_evt.events & (EPOLLOUT | EPOLLERR | EPOLLHUP))) {
      out = std::move(handle->prom_out);
      handle->prom_out.reset();
      --pending_consumers;
    }
    // Track handle update and return the task
    schedule_update(handle);
    return [in = std::move(in), out = std::move(out)] mutable {
      if (in.has_value())
        std::move(*in).fulfill();
      if (out.has_value())
        std::move(*out).fulfill();
    };
  }
  return std::nullopt;
}

bool IOQueue::empty() const {
  return pending_consumers == 0;
}

IOQueue::~IOQueue() {
  close(ep_fd);
}

} // namespace AIO
