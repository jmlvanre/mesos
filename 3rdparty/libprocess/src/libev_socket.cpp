#include <arpa/inet.h>

#include <netinet/in.h>
#include <netinet/tcp.h>

#include <process/socket.hpp>

#include "libev.hpp"

namespace process {

class LibevImpl : public Socket::Impl
{
public:
  LibevImpl(int _s) : Socket::Impl(_s) {}

  virtual ~LibevImpl() {}

  virtual Future<Socket> connect(const Node& node);

  virtual Future<size_t> read(char* data, size_t length);

  virtual Future<size_t> send(const char* data, size_t length);

  virtual Future<size_t> sendFile(int fd, off_t offset, size_t length);

  virtual Try<Node> bind(const Node& node);

  virtual Try<Nothing> listen(int backlog);

  virtual Future<Socket> accept();

  private:

  void startRead(void (*func)(struct ev_loop*, ev_io*, int), void* data)
  {
    readWatcher.data = data;
    ev_io_init(&readWatcher, func, s, EV_READ);
    synchronized (watchers) {
      watchers->push(&readWatcher);
    }
    ev_async_send(loop, &async_watcher);
  }

  void startWrite(void (*func)(struct ev_loop*, ev_io*, int), void* data)
  {
    writeWatcher.data = data;
    ev_io_init(&writeWatcher, func, s, EV_WRITE);
    synchronized (watchers) {
      watchers->push(&writeWatcher);
    }
    ev_async_send(loop, &async_watcher);
  }

  ev_io readWatcher;
  ev_io writeWatcher;
};


Socket::Socket(int _s) : impl(std::make_shared<LibevImpl>(_s)) {}


std::shared_ptr<Socket::Impl> Socket::create()
{
  Try<int> fd = process::socket(
      AF_INET,
      SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
      0);
  if (fd.isError()) {
    ABORT("Failed to create socket: " + fd.error());
  }
  return std::make_shared<LibevImpl>(fd.get());
}

namespace internal {

struct Connect
{
  Connect(const Socket& _socket) : socket(_socket) {}
  const Socket socket;
  Promise<Socket> promise;
};

void connect_ready(struct ev_loop* loop, ev_io* watcher, int revents)
{
  Connect* connect = reinterpret_cast<Connect*>(watcher->data);

  ev_io_stop(loop, watcher);
  // Now check that a successful connection was made.
  int opt;
  socklen_t optlen = sizeof(opt);
  int s = watcher->fd;

  if (getsockopt(s, SOL_SOCKET, SO_ERROR, &opt, &optlen) < 0 || opt != 0) {
    // Connect failure.
    VLOG(1) << "Socket error while connecting";
    connect->promise.fail("Socket error while connecting");
  } else {
    // We're connected! Let's satisfy our promise.
    connect->promise.set(connect->socket);
  }
  delete connect;
}

}

Future<Socket> LibevImpl::connect(const Node& node)
{
  CHECK(s > 0) << "Connect requires an initialized socket.";

  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = PF_INET;
  addr.sin_port = htons(node.port);
  addr.sin_addr.s_addr = node.ip;

  if (::connect(s, (sockaddr*) &addr, sizeof(addr)) < 0) {
    if (errno != EINPROGRESS) {
      return Failure(ErrnoError("Failed to connect socket"));
    }

    internal::Connect* connect = new internal::Connect(socket());

    auto result = connect->promise.future();

    startWrite(internal::connect_ready, connect);

    return result;
  }
  return socket();
}

namespace internal {

struct Read
{
  Read(const Socket& _socket, char* _data, size_t _size) : socket(_socket), data(_data), size(_size) {}
  const Socket socket;
  char* data;
  size_t size;
  Promise<size_t> promise;
};

void read_ready(struct ev_loop* loop, ev_io* watcher, int revents)
{
  Read* read = reinterpret_cast<Read*>(watcher->data);

  int s = watcher->fd;

  while (true) {
    ssize_t length = recv(s, read->data, read->size, 0);
    if (length < 0 && (errno == EINTR)) {
      // Interrupted, try again now.
      continue;
    } else if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Might block, try again later.
      break;
    } else if (length < 0) {
      // Socket error or closed.
      ev_io_stop(loop, watcher);
      read->promise.fail(strerror(errno));
      delete read;
      break;
    } else {
      ev_io_stop(loop, watcher);
      read->promise.set(length);
      delete read;
      break;
    }
  }
}

} // namespace internal {

Future<size_t> LibevImpl::read(char* data, size_t length)
{
  CHECK(s > 0) << "Read requires an initialized socket.";

  internal::Read* read = new internal::Read(socket(), data, length);

  auto result = read->promise.future();

  startRead(internal::read_ready, read);
  return result;
}

namespace internal {

struct Send
{
  Send(const Socket& _socket, const char* _data, size_t _size) : socket(_socket), data(_data), size(_size) {}
  const Socket socket;
  const char* data;
  size_t size;
  Promise<size_t> promise;
};


void send_ready(struct ev_loop* loop, ev_io* watcher, int revents)
{
  Send* send = reinterpret_cast<Send*>(watcher->data);

  int s = watcher->fd;

  while (true) {
    ssize_t length = ::send(s, send->data, send->size, MSG_NOSIGNAL);
    if (length < 0 && (errno == EINTR)) {
      // Interrupted, try again now.
      continue;
    } else if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Might block, try again later.
      break;
    } else if (length < 0) {
      // Socket error or closed.
      ev_io_stop(loop, watcher);
      send->promise.fail(strerror(errno));
      delete send;
      break;
    } else {
      ev_io_stop(loop, watcher);
      send->promise.set(length);
      delete send;
      break;
    }
  }
}


struct SendFile
{
  SendFile(const Socket& _socket, int _fd, off_t _offset, size_t _size)
    : socket(_socket), fd(_fd), offset(_offset), size(_size) {}
  const Socket socket;
  int fd;
  off_t offset;
  size_t size;
  Promise<size_t> promise;
};


void send_file_ready(struct ev_loop* loop, ev_io* watcher, int revents)
{
  SendFile* sendFile = reinterpret_cast<SendFile*>(watcher->data);

  int s = watcher->fd;

  while (true) {
    ssize_t length =
      os::sendfile(s, sendFile->fd, sendFile->offset, sendFile->size);
    if (length < 0 && (errno == EINTR)) {
      // Interrupted, try again now.
      continue;
    } else if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Might block, try again later.
      break;
    } else if (length < 0) {
      // Socket error or closed.
      ev_io_stop(loop, watcher);
      sendFile->promise.fail(strerror(errno));
      delete sendFile;
      break;
    } else {
      ev_io_stop(loop, watcher);
      sendFile->promise.set(length);
      delete sendFile;
      break;
    }
  }
}

} // namespace internal {

Future<size_t> LibevImpl::send(const char* data, size_t length)
{
  CHECK(s > 0) << "Send requires an initialized socket.";

  internal::Send* send = new internal::Send(socket(), data, length);

  auto result = send->promise.future();

  startWrite(internal::send_ready, send);
  return result;
}

Future<size_t> LibevImpl::sendFile(int fd, off_t offset, size_t length)
{
  CHECK(s > 0) << "SendFile requires an initialized socket.";

  internal::SendFile* sendFile = new internal::SendFile(socket(), fd, offset, length);

  auto result = sendFile->promise.future();

  startWrite(internal::send_file_ready, sendFile);
  return result;
}

Try<Node> LibevImpl::bind(const Node& node)
{
  CHECK(s > 0) << "Bind requires an initialized socket.";

  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = PF_INET;
  addr.sin_addr.s_addr = node.ip;
  addr.sin_port = htons(node.port);

  if (::bind(s, (sockaddr*) &addr, sizeof(addr)) < 0) {
    return Error("Failed to bind: " + stringify(inet_ntoa(addr.sin_addr)) +
      ":" + stringify(node.port));
  }

  // Lookup and store assigned ip and assigned port.
  socklen_t addrlen = sizeof(addr);
  if (getsockname(s, (sockaddr*) &addr, &addrlen) < 0) {
    return Error("Failed to bind, getsockname");
  }

  return Node(addr.sin_addr.s_addr, ntohs(addr.sin_port));
}

Try<Nothing> LibevImpl::listen(int backlog)
{
  CHECK(s > 0) << "Listen requires an initialized socket.";

  if (::listen(s, backlog) < 0) {
    return ErrnoError();
  }
  return Nothing();
}

namespace internal {

struct Accept
{
  Accept(const Socket& _socket) : socket(_socket) {}
  const Socket socket;
  Promise<Socket> promise;
};

void accept_ready(struct ev_loop* loop, ev_io* watcher, int revents)
{
  Accept* accept = reinterpret_cast<Accept*>(watcher->data);

  sockaddr_in addr;
  socklen_t addrlen = sizeof(addr);

  int clientSocket = ::accept(watcher->fd, (sockaddr*) &addr, &addrlen);

  if (clientSocket < 0) {
    if (!(errno == EAGAIN || errno == EWOULDBLOCK)) {
      ev_io_stop(loop, watcher);
      accept->promise.fail("Failed to accept");
      delete accept;
    }
    return;
  }

  ev_io_stop(loop, watcher);

  Try<Nothing> nonblock = os::nonblock(clientSocket);
  if (nonblock.isError()) {
    LOG_IF(INFO, VLOG_IS_ON(1)) << "Failed to accept, nonblock: "
                                << nonblock.error();
    os::close(clientSocket);
    accept->promise.fail("Failed to accept, nonblock: " + nonblock.error());
    delete accept;
    return;
  }

  Try<Nothing> cloexec = os::cloexec(clientSocket);
  if (cloexec.isError()) {
    LOG_IF(INFO, VLOG_IS_ON(1)) << "Failed to accept, cloexec: "
                                << cloexec.error();
    os::close(clientSocket);
    accept->promise.fail("Failed to accept, cloexec: " + cloexec.error());
    delete accept;
    return;
  }

  // Turn off Nagle (TCP_NODELAY) so pipelined requests don't wait.
  int on = 1;
  if (setsockopt(clientSocket, SOL_TCP, TCP_NODELAY, &on, sizeof(on)) < 0) {
    const char* error = strerror(errno);
    VLOG(1) << "Failed to turn off the Nagle algorithm: " << error;
    os::close(clientSocket);
    accept->promise.fail(
      "Failed to turn off the Nagle algorithm: " + stringify(error));
    delete accept;
  } else {
    accept->promise.set(Socket(clientSocket));
    delete accept;
  }
}

}

Future<Socket> LibevImpl::accept()
{
  CHECK(s > 0) << "Accept requires an initialized socket.";

  internal::Accept* accept = new internal::Accept(socket());

  auto result = accept->promise.future();

  startRead(internal::accept_ready, accept);

  return result;
}

} // namespace process