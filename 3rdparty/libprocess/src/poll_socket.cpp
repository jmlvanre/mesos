#include <arpa/inet.h>

#include <netinet/in.h>
#include <netinet/tcp.h>

#include <process/io.hpp>
#include <process/socket.hpp>

namespace process {

class PollImpl : public Socket::Impl
{
public:
  PollImpl(int _s) : Socket::Impl(_s) {}

  virtual ~PollImpl() {}

  virtual Future<Socket> connect(const Node& node);

  virtual Future<size_t> read(char* data, size_t length);

  virtual Future<size_t> send(const char* data, size_t length);

  virtual Future<size_t> sendFile(int fd, off_t offset, size_t length);

  virtual Try<Node> bind(const Node& node);

  virtual Try<Nothing> listen(int backlog);

  virtual Future<Socket> accept();
};


Socket::Socket(int _s) : impl(std::make_shared<PollImpl>(_s)) {}


std::shared_ptr<Socket::Impl> Socket::create()
{
  Try<int> fd = process::socket(
      AF_INET,
      SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
      0);
  if (fd.isError()) {
    ABORT("Failed to create socket: " + fd.error());
  }
  return std::make_shared<PollImpl>(fd.get());
}


namespace internal {

struct Connect
{
  Promise<Socket> promise;
};

void connect(const Socket& socket, Connect* _connect)
{
  // Now check that a successful connection was made.
  int opt;
  socklen_t optlen = sizeof(opt);
  int s = socket;

  if (getsockopt(s, SOL_SOCKET, SO_ERROR, &opt, &optlen) < 0 || opt != 0) {
    // Connect failure.
    VLOG(1) << "Socket error while connecting";
    _connect->promise.fail("Socket error while connecting");
  } else {
    // We're connected! Let's satisfy our promise.
    _connect->promise.set(socket);
  }
  delete _connect;
}

} // namespace internal {


Future<Socket> PollImpl::connect(const Node& node)
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

    internal::Connect* connect = new internal::Connect();

    auto result = connect->promise.future();

    io::poll(s, io::WRITE)
      .onAny(lambda::bind(
                 &internal::connect,
                 socket(),
                 connect));

    return result;
  }
  return socket();
}


Future<size_t> PollImpl::read(char* data, size_t length)
{
  CHECK(s > 0) << "Read requires an initialized socket.";

  return io::read(s, data, length);
}


namespace internal {

struct SendRequest {
  SendRequest(const char* _data, size_t _size) : data(_data), size(_size) {}
  const char* data;
  size_t size;
  Promise<size_t> promise;
};


void socket_send_data(
    int s,
    const char* data,
    size_t size,
    SendRequest* request)
{
  CHECK(size > 0);

  while (true) {
    ssize_t length = send(s, data, size, MSG_NOSIGNAL);

    if (length < 0 && (errno == EINTR)) {
      // Interrupted, try again now.
      continue;
    } else if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Might block, try again later.
      io::poll(s, io::WRITE)
        .onAny(lambda::bind(
            &internal::socket_send_data,
            s,
            data,
            size,
            request));
      return;
    } else if (length <= 0) {
      // Socket error or closed.
      if (length < 0) {
        const char* error = strerror(errno);
        VLOG(1) << "Socket error while sending: " << error;
      } else {
        VLOG(1) << "Socket closed while sending";
      }
      if (length == 0) {
        request->promise.set(length);
      } else {
        request->promise.fail("Socket send failed");
      }
      delete request;
      return;
    } else {
      CHECK(length > 0);

      request->promise.set(length);
      delete request;
      return;
    }
  }
}


struct SendFileRequest {
  SendFileRequest(int _fd, off_t _offset, size_t _size)
    : fd(_fd), offset(_offset), size(_size) {}
  int fd;
  off_t offset;
  size_t size;
  Promise<size_t> promise;
};

void socket_send_file(int s, SendFileRequest* request)
{
  CHECK(request->size > 0);

  while (true) {
    ssize_t length =
      os::sendfile(s, request->fd, request->offset, request->size);

    if (length < 0 && (errno == EINTR)) {
      // Interrupted, try again now.
      continue;
    } else if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Might block, try again later.
      io::poll(s, io::WRITE)
        .onAny(lambda::bind(&internal::socket_send_file, s, request));
      return;
    } else if (length <= 0) {
      // Socket error or closed.
      if (length < 0) {
        const char* error = strerror(errno);
        VLOG(1) << "Socket error while sending: " << error;
      } else {
        VLOG(1) << "Socket closed while sending";
      }
      if (length == 0) {
        request->promise.set(length);
      } else {
        request->promise.fail("Socket sendFile failed");
      }
      delete request;
      return;
    } else {
      CHECK(length > 0);

      request->promise.set(length);
      delete request;
      return;
    }
  }
}

} // namespace internal {


Future<size_t> PollImpl::send(const char* data, size_t length)
{
  CHECK(s > 0) << "Send requires an initialized socket.";

  internal::SendRequest* request = new internal::SendRequest(data, length);

  auto future = request->promise.future();

  io::poll(s, io::WRITE)
    .onAny(lambda::bind(&internal::socket_send_data, s, data, length, request));
  return future;
}


Future<size_t> PollImpl::sendFile(int fd, off_t offset, size_t length)
{
  CHECK(s > 0) << "SendFile requires an initialized socket.";

  internal::SendFileRequest* request =
    new internal::SendFileRequest(fd, offset, length);

  auto future = request->promise.future();

  io::poll(s, io::WRITE)
    .onAny(lambda::bind(&internal::socket_send_file, s, request));
  return future;
}


Try<Node> PollImpl::bind(const Node& node)
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


Try<Nothing> PollImpl::listen(int backlog)
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
  Accept(int _fd) : fd(_fd) {}
  int fd;
  Promise<Socket> promise;
};


void accept(Accept* _accept)
{
  sockaddr_in addr;
  socklen_t addrlen = sizeof(addr);

  int clientSocket = ::accept(_accept->fd, (sockaddr*) &addr, &addrlen);

  if (clientSocket < 0) {
    _accept->promise.fail("Failed to accept");
    delete _accept;
    return;
  }

  Try<Nothing> nonblock = os::nonblock(clientSocket);
  if (nonblock.isError()) {
    LOG_IF(INFO, VLOG_IS_ON(1)) << "Failed to accept, nonblock: "
                                << nonblock.error();
    os::close(clientSocket);
    _accept->promise.fail("Failed to accept, nonblock: " + nonblock.error());
    delete _accept;
    return;
  }

  Try<Nothing> cloexec = os::cloexec(clientSocket);
  if (cloexec.isError()) {
    LOG_IF(INFO, VLOG_IS_ON(1)) << "Failed to accept, cloexec: "
                                << cloexec.error();
    os::close(clientSocket);
    _accept->promise.fail("Failed to accept, cloexec: " + cloexec.error());
    delete _accept;
    return;
  }

  // Turn off Nagle (TCP_NODELAY) so pipelined requests don't wait.
  int on = 1;
  if (setsockopt(clientSocket, SOL_TCP, TCP_NODELAY, &on, sizeof(on)) < 0) {
    const char* error = strerror(errno);
    VLOG(1) << "Failed to turn off the Nagle algorithm: " << error;
    os::close(clientSocket);
    _accept->promise.fail(
      "Failed to turn off the Nagle algorithm: " + stringify(error));
    delete _accept;
  } else {
    _accept->promise.set(Socket(clientSocket));
    delete _accept;
  }
}

} // namespace internal {


Future<Socket> PollImpl::accept()
{
  CHECK(s > 0) << "Accept requires an initialized socket.";

  internal::Accept* accept =
    new internal::Accept(s);

  auto future = accept->promise.future();

  io::poll(s, io::READ)
      .onAny(lambda::bind(
                 &internal::accept,
                 accept));
  return future;
}

} // namespace process {
