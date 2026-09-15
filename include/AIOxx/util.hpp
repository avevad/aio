#pragma once

#include <exception>
#include <expected>
#include <iostream>
#include <optional>
#include <source_location>
#include <string>
#include <utility>

#ifdef AIOXX_DEBUG
#define AIOXX_ASSUME(WHAT)                                                                                             \
  do {                                                                                                                 \
    if (!(WHAT)) {                                                                                                     \
      AIO::panic(std::string("assumption failed: ") + #WHAT);                                                          \
    }                                                                                                                  \
  } while (0)
#else
#define AIOXX_ASSUME(WHAT) [[assume(WHAT)]]
#endif

#define AIOXX_UNREACHABLE                                                                                              \
  do {                                                                                                                 \
    AIOXX_ASSUME(false);                                                                                               \
    std::unreachable();                                                                                                \
  } while (0)

namespace AIO {

[[noreturn]] void panic(const std::string &what, std::source_location where = std::source_location::current());

[[noreturn]] void
panic(const std::string &what, std::exception_ptr err, std::source_location where = std::source_location::current());

void warning(const std::string &what, std::source_location where = std::source_location::current());

void warning(
  const std::string &what, std::exception_ptr err, std::source_location where = std::source_location::current()
);

template<typename Derived, typename Derived1, bool Master>
class Bond {
public:
  Bond() = default;

  Bond(const Bond &) = delete;
  Bond &operator=(const Bond &) = delete;

  Bond(Bond &&other) noexcept;
  Bond &operator=(Bond &&other) noexcept;

  ~Bond();

protected:
  void initialize(Derived1 &bound);

  bool is_initialized() const;
  bool is_alive() const;

  Derived1 *get_ptr() const;
  Derived1 &get() const;

private:
  friend class Bond<Derived1, Derived, !Master>;

  Bond<Derived1, Derived, !Master> *get_base_ptr();

  std::optional<Derived1 *> maybe_ptr = std::nullopt;
};

template<typename T, bool Master>
class BoundStorage : public Bond<BoundStorage<T, Master>, BoundStorage<T, !Master>, Master> {
  using Bond = Bond<BoundStorage, BoundStorage<T, !Master>, Master>;

public:
  BoundStorage() = default;

  BoundStorage(BoundStorage &&) noexcept = default;
  BoundStorage &operator=(BoundStorage &&) noexcept = default;

  BoundStorage(const BoundStorage &) = delete;
  BoundStorage &operator=(const BoundStorage &) = delete;

  ~BoundStorage();

  void bind_to(BoundStorage<T, !Master> &bound);

  template<typename T1>
  void set(T1 &&value);

  T &get();
  const T &get() const;

private:
  friend class BoundStorage<T, !Master>;

  std::optional<T> maybe_value = std::nullopt;
};

template<typename Res>
using Expected = std::expected<Res, std::exception_ptr>;

template<typename Res>
struct ExpectedResult {
  using Result = Res;
  using Expected = Expected<Res>;
  template<typename Res1>
  using Mapped = ExpectedResult<Res1>;

  bool is_ok() const;

  decltype(auto) move_as_ok();
  auto move_as_err();

  template<typename R = Res>
  static ExpectedResult make_ok(R res)
    requires(!std::is_void_v<Res>);
  static ExpectedResult make_ok()
    requires(std::is_void_v<Res>);

  template<typename Exception>
  static ExpectedResult make_err_from(const Exception &e);
  static ExpectedResult make_err(std::exception_ptr err);
  static ExpectedResult make_err_from_current();

  Expected expected;
};

} // namespace AIO


// --------------------------------------------------
// -------------- TEMPLATE DEFINITIONS --------------
// --------------------------------------------------


namespace AIO {
template<typename Derived, typename Derived1, bool Master>
Bond<Derived, Derived1, Master>::Bond(Bond &&other) noexcept : maybe_ptr(other.maybe_ptr) {
  other.maybe_ptr.reset();
  if (auto bound = get_base_ptr()) {
    bound->maybe_ptr = static_cast<Derived *>(this);
  }
}

template<typename Derived, typename Derived1, bool Master>
Bond<Derived, Derived1, Master> &Bond<Derived, Derived1, Master>::operator=(Bond &&other) noexcept {
  if (&other == this) {
    return *this;
  }

  if (auto bound = get_base_ptr()) {
    bound->maybe_ptr = nullptr;
  }

  maybe_ptr = other.maybe_ptr;
  other.maybe_ptr.reset();
  if (auto bound = get_base_ptr()) {
    bound->maybe_ptr = static_cast<Derived *>(this);
  }

  return *this;
}

template<typename Derived, typename Derived1, bool Master>
Bond<Derived, Derived1, Master>::~Bond() {
  if (auto bound = get_base_ptr()) {
    bound->maybe_ptr = nullptr;
  }
}

template<typename Derived, typename Derived1, bool Master>
void Bond<Derived, Derived1, Master>::initialize(Derived1 &bound) {
  AIOXX_ASSUME(!is_initialized());
  AIOXX_ASSUME(!bound.is_initialized());
  maybe_ptr = &bound;
  bound.maybe_ptr = static_cast<Derived *>(this);
}

template<typename Derived, typename Derived1, bool Master>
bool Bond<Derived, Derived1, Master>::is_initialized() const {
  return maybe_ptr.has_value();
}

template<typename Derived, typename Derived1, bool Master>
bool Bond<Derived, Derived1, Master>::is_alive() const {
  AIOXX_ASSUME(is_initialized());
  return maybe_ptr != nullptr;
}

template<typename Derived, typename Derived1, bool Master>
Derived1 *Bond<Derived, Derived1, Master>::get_ptr() const {
  AIOXX_ASSUME(is_initialized());
  return *maybe_ptr;
}

template<typename Derived, typename Derived1, bool Master>
Derived1 &Bond<Derived, Derived1, Master>::get() const {
  AIOXX_ASSUME(is_alive());
  return **maybe_ptr;
}

template<typename Derived, typename Derived1, bool Master>
Bond<Derived1, Derived, !Master> *Bond<Derived, Derived1, Master>::get_base_ptr() {
  return maybe_ptr.has_value() ? static_cast<Bond<Derived1, Derived, !Master> *>(*maybe_ptr) : nullptr;
}

template<typename T, bool Master>
BoundStorage<T, Master>::~BoundStorage() {
  if constexpr (Master) {
    if (maybe_value.has_value() && Bond::is_initialized() && Bond::is_alive()) {
      auto &slave = Bond::get();
      slave.maybe_value = std::move(maybe_value);
      maybe_value.reset();
    }
  }
}

template<typename T, bool Master>
void BoundStorage<T, Master>::bind_to(BoundStorage<T, !Master> &bound) {
  AIOXX_ASSUME(!maybe_value.has_value());
  AIOXX_ASSUME(!bound.maybe_value.has_value());
  Bond::initialize(bound);
}

template<typename T, bool Master>
template<typename T1>
void BoundStorage<T, Master>::set(T1 &&value) {
  AIOXX_ASSUME(Bond::is_initialized());

  if constexpr (Master) {
    maybe_value = std::forward<T1>(value);
  } else {
    if (Bond::is_alive()) {
      Bond::get().set(std::forward<T1>(value));
    } else {
      maybe_value = std::forward<T1>(value);
    }
  }
}

template<typename T, bool Master>
T &BoundStorage<T, Master>::get() {
  AIOXX_ASSUME(Bond::is_initialized());

  if constexpr (Master) {
    AIOXX_ASSUME(maybe_value.has_value());
    return *maybe_value;
  } else {
    if (Bond::is_alive()) {
      return Bond::get().get();
    }
    AIOXX_ASSUME(maybe_value.has_value());
    return *maybe_value;
  }
}

template<typename T, bool Master>
const T &BoundStorage<T, Master>::get() const {
  AIOXX_ASSUME(Bond::is_initialized());

  if constexpr (Master) {
    AIOXX_ASSUME(maybe_value.has_value());
    return *maybe_value;
  } else {
    if (Bond::is_alive()) {
      return Bond::get().get();
    }
    AIOXX_ASSUME(maybe_value.has_value());
    return *maybe_value;
  }
}

template<typename Res>
bool ExpectedResult<Res>::is_ok() const {
  return expected.has_value();
}

template<typename Res>
decltype(auto) ExpectedResult<Res>::move_as_ok() {
  AIOXX_ASSUME(is_ok());
  if constexpr (std::is_void_v<Res>) {
  } else {
    return std::move(*expected);
  }
}

template<typename Res>
auto ExpectedResult<Res>::move_as_err() {
  AIOXX_ASSUME(!is_ok());
  return std::move(expected.error());
}

template<typename Res>
template<typename R>
ExpectedResult<Res> ExpectedResult<Res>::make_ok(R res)
  requires(!std::is_void_v<Res>)
{
  return {.expected = std::move(res)};
}

template<typename Res>
ExpectedResult<Res> ExpectedResult<Res>::make_ok()
  requires(std::is_void_v<Res>)
{
  return {.expected = {}};
}

template<typename Res>
ExpectedResult<Res> ExpectedResult<Res>::make_err(std::exception_ptr err) {
  AIOXX_ASSUME(err != nullptr);
  return {.expected = std::unexpected(std::move(err))};
}

template<typename Res>
ExpectedResult<Res> ExpectedResult<Res>::make_err_from_current() {
  return make_err(std::current_exception());
}

template<typename Res>
template<typename Exception>
ExpectedResult<Res> ExpectedResult<Res>::make_err_from(const Exception &e) {
  try {
    throw e;
  } catch (...) {
    return make_err_from_current();
  }
}
} // namespace AIO
