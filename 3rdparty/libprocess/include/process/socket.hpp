#ifndef __PROCESS_SOCKET_HPP__
#define __PROCESS_SOCKET_HPP__

#include <assert.h>

#include <mutex>

#include <process/future.hpp>
#include <process/node.hpp>

#include <stout/abort.hpp>
#include <stout/nothing.hpp>
#include <stout/memory.hpp>
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
  enum connectionState {
    notConnected,
    establishingConnection,
    connected,
    connectionClosed,
    connectionFailed
  };

  enum socketKind {
    libev,
    uninitialized
  };

  class Impl : public std::enable_shared_from_this<Impl>
  {
  public:
    virtual ~Impl() {}

    operator int () const
    {
      return s;
    }

    virtual Future<Socket> connect(const Node& node) = 0;

    virtual Future<Socket> onConnected() = 0;

    virtual void onRead(const std::function<void (const char*, size_t)>& cb) = 0;
    virtual void readReady(const char* data, size_t length) = 0;

    virtual void setConnectionState(const connectionState connState) = 0;

    virtual void onClose(const std::function<void (const Socket& socket)>& cb) = 0;
    virtual void closeSocket() = 0;
    virtual void drainAndClose() = 0;

    virtual Future<Nothing> send(const char* data, size_t length) = 0;
    virtual Future<Nothing> sendFile(int fd, size_t length) = 0;
    virtual void trySend() = 0;

  protected:
    Impl(int _s, connectionState connState) : s(_s), _connectionState(connState) {}

    Socket socket() {
      return Socket(shared_from_this());
    }

    int s;
    connectionState _connectionState;
  };

  Socket(socketKind _kind = uninitialized) : kind(_kind) {
    if (kind == libev) {
      get();
    }
  }

  explicit Socket(int _s, socketKind kind, connectionState _connectionState = notConnected);

  bool operator == (const Socket& that) const
  {
    return impl == that.impl;
  }

  operator int () const
  {
    return *get();
  }

  Future<Socket> connect(const Node& node, socketKind kind)
  {
    return get(kind)->connect(node);
  }

  Future<Socket> onConnected()
  {
    CHECK(impl) << "Socket must be initialized to call onConnected()";
    return impl->onConnected();
  }

  const Socket& onRead(const std::function<void (const char*, size_t)>& cb) const
  {
    CHECK(impl) << "Socket must be initialized to call onRead()";
    impl->onRead(cb);
    return *this;
  }

  const Socket& onClose(const std::function<void (const Socket& socket)>& cb) const
  {
    CHECK(impl) << "Socket must be initialized to call onClose()";
    impl->onClose(cb);
    return *this;
  }

  void close()
  {
    CHECK(impl) << "Socket must be initialized to call close()";
    return impl->closeSocket();
  }

  Future<Nothing> send(const char* data, size_t length) const
  {
    CHECK(impl) << "Socket must be initialized to call send()";
    return impl->send(data, length);
  }

  Future<Nothing> sendFile(int fd, size_t length) const
  {
    CHECK(impl) << "Socket must be initialized to call sendFile()";
    return impl->sendFile(fd, length);
  }

  void drainAndClose() const
  {
    CHECK(impl) << "Socket must be initialized to call drainAndClose()";
    impl->drainAndClose();
  }

private:
  Socket(std::shared_ptr<Impl>&& _impl) : impl(std::move(_impl)) {}

  const std::shared_ptr<Impl>& get() const
  {
    return impl ? impl : (impl = create(kind));
  }

  const std::shared_ptr<Impl>& get(socketKind _kind) const
  {
    return impl ? impl : (impl = create(_kind));
  }

  static std::shared_ptr<Impl> create(socketKind _kind);

  socketKind kind;
  mutable std::shared_ptr<Impl> impl;
};

} // namespace process {

#endif // __PROCESS_SOCKET_HPP__
