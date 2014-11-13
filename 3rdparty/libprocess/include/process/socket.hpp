#ifndef __PROCESS_SOCKET_HPP__
#define __PROCESS_SOCKET_HPP__

#include <assert.h>

#include <memory>

#include <process/future.hpp>
#include <process/node.hpp>

#include <stout/abort.hpp>
#include <stout/memory.hpp>
#include <stout/nothing.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>


namespace process {

// Returns a socket fd for the specified options. Note that on OS X,
// the returned socket will have the SO_NOSIGPIPE option set.
inline Try<int> socket(int family, int type, int protocol) {
  int s;
  if ((s = ::socket(family, type, protocol)) == -1) {
    return ErrnoError();
  }

#ifdef __APPLE__
  // Disable SIGPIPE via setsockopt because OS X does not support
  // the MSG_NOSIGNAL flag on send(2).
  const int enable = 1;
  if (setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &enable, sizeof(int)) == -1) {
    return ErrnoError();
  }
#endif // __APPLE__

  return s;
}


// An abstraction around a socket (file descriptor) that provides
// reference counting such that the socket is only closed (and thus,
// has the possiblity of being reused) after there are no more
// references.

class Socket
{
public:
  class Impl : public std::enable_shared_from_this<Impl>
  {
  public:
    Impl() : s(-1) {}

    Impl(int _s) : s(_s) {}

    ~Impl()
    {
      if (s >= 0) {
        Try<Nothing> close = os::close(s);
        if (close.isError()) {
          ABORT("Failed to close socket: " + close.error());
        }
      }
    }

    operator int () const
    {
      return s >= 0 ? s : static_cast<int>(get());
    }

    Future<Socket> connect(const Node& node);

    Future<size_t> read(char* data, size_t length);

  private:
    const Impl& get() const
    {
      return s >= 0 ? *this : create();
    }

    const Impl& create() const
    {
      CHECK(s < 0);
      Try<int> fd =
        process::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
      if (fd.isError()) {
        ABORT("Failed to create socket: " + fd.error());
      }
      s = fd.get();
      return *this;
    }

    // Mutable so that the socket can be lazily created.
    mutable int s;
  };

  Socket() : impl(std::make_shared<Impl>()) {}

  explicit Socket(int s) : impl(std::make_shared<Impl>(s)) {}

  bool operator == (const Socket& that) const
  {
    return impl == that.impl;
  }

  operator int () const
  {
    return *impl;
  }

  Future<Socket> connect(const Node& node)
  {
    return impl->connect(node);
  }

  Future<size_t> read(char* data, size_t length) const
  {
    return impl->read(data, length);
  }

private:
  explicit Socket(memory::shared_ptr<Impl>&& that) : impl(std::move(that)) {}

  memory::shared_ptr<Impl> impl;
};

} // namespace process {

#endif // __PROCESS_SOCKET_HPP__
