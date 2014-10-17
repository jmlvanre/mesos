#ifndef __PROCESS_SOCKET_HPP__
#define __PROCESS_SOCKET_HPP__

#include <assert.h>

#include <memory>

#include <stout/abort.hpp>
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
  Socket() {}

  explicit Socket(int _s)
    : impl(std::make_shared<Impl>(_s)) {}

  bool operator == (const Socket& that) const
  {
    return impl == that.impl;
  }

  operator int () const
  {
    return impl ? static_cast<int>(*impl) : -1;
  }

private:
  class Impl {
  public:
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
      return s;
    }

  private:
    int s;
  };

  std::shared_ptr<Impl> impl;
};

} // namespace process {

#endif // __PROCESS_SOCKET_HPP__
