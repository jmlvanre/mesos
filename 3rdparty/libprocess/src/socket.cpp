#include <process/socket.hpp>

using std::string;

namespace process {
namespace network {

// Forward declarations.
Try<std::shared_ptr<Socket::Impl>> pollSocket(int s);
Try<std::shared_ptr<Socket::Impl>> libeventSocket(int s);
Try<std::shared_ptr<Socket::Impl>> libeventSSLSocket(int s, void* arg);


Try<Node> Socket::Impl::bind(const Node& node)
{
  Try<int> bind = network::bind(get(), node);
  if (bind.isError()) {
    return Error(bind.error());
  }

  std::stringstream ss;
  ss << network::getsockname(get(), AF_INET).get();
  // Lookup and store assigned ip and assigned port.
  return network::getsockname(get(), AF_INET);
}


Socket::Kind getDefaultSocketKind() {
  const char* env = getenv("USE_SSL");
  if (env != NULL && strcmp(env, "1") == 0) {
    return Socket::SSL;
  } else {
#ifdef USE_LIBEVENT_SOCKET
    return Socket::LIBEVENT;
#else
    return Socket::POLL;
#endif
  }
}


const Socket::Kind& Socket::DEFAULT_SOCKET_KIND()
{
  // TODO(jmlvanre): Change the default based on configure or
  // environment flags.
  static Kind defaultKind = getDefaultSocketKind();
  return defaultKind;
}


Try<Socket> Socket::create(Kind kind, int s, void* arg)
{
  if (s < 0) {
    // Supported in Linux >= 2.6.27.
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    Try<int> fd =
      network::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

    if (fd.isError()) {
      return Error("Failed to create socket: " + fd.error());
    }
#else
    Try<int> fd = network::socket(AF_INET, SOCK_STREAM, 0);
    if (fd.isError()) {
      return Error("Failed to create socket: " + fd.error());
    }

    Try<Nothing> nonblock = os::nonblock(fd.get());
    if (nonblock.isError()) {
      return Error("Failed to create socket, nonblock: " + nonblock.error());
    }

    Try<Nothing> cloexec = os::cloexec(fd.get());
    if (cloexec.isError()) {
      return Error("Failed to create socket, cloexec: " + cloexec.error());
    }
#endif

    s = fd.get();
  }

  switch (kind) {
    case POLL: {
      Try<std::shared_ptr<Socket::Impl>> socket = pollSocket(s);
      if (socket.isError()) {
        return Error(socket.error());
      }
      return Socket(socket.get());
    }
#ifdef USE_LIBEVENT_SOCKET
    case LIBEVENT: {
      Try<std::shared_ptr<Socket::Impl>> socket = libeventSocket(s);
      if (socket.isError()) {
        return Error(socket.error());
      }
      return Socket(socket.get());
    }
#endif
#ifdef USE_SSL_SOCKET
    case SSL: {
      Try<std::shared_ptr<Socket::Impl>> socket = libeventSSLSocket(s, arg);
      if (socket.isError()) {
        return Error(socket.error());
      }
      return Socket(socket.get());
    }
#endif
    // By not setting a default we leverage the compiler errors when
    // the enumeration is augmented to find all the cases we need to
    // provide.
  }

  return Error("Unreachable, all socket kinds should be in switch statement");
}

} // namespace network {
} // namespace process {
