#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>

#include <process/socket.hpp>

#include "libevent.hpp"

namespace process {
namespace network {

class LibeventSocketImpl : public Socket::Impl
{
public:
  LibeventSocketImpl(struct bufferevent* _bev, int _s);

  virtual ~LibeventSocketImpl() {
    bufferevent_free(bev);
    if (listener != NULL) {
      evconnlistener_free(listener);
    }
  }

  virtual Future<Nothing> connect(const Node& node);

  virtual Future<size_t> recv(char* data, size_t size);

  virtual Future<size_t> send(const char* data, size_t size);

  virtual Future<size_t> sendfile(int fd, off_t offset, size_t size);

  virtual Try<Nothing> listen(int backlog);

  virtual Future<Socket> accept();

private:
  struct RecvRequest
  {
    RecvRequest(char* _data, size_t _size) : data(_data), size(_size) {}
    Promise<size_t> promise;
    char* data;
    size_t size;
  };

  struct SendRequest
  {
    SendRequest(size_t _size) : size(_size) {}
    Promise<size_t> promise;
    size_t size;
  };

  struct ConnectRequest
  {
    Promise<Nothing> promise;
  };

  static void recvCb(struct bufferevent* bev, void* arg);

  static void sendCb(struct bufferevent* bev, void* arg);

  static void eventCb(struct bufferevent* bev, short events, void* arg);

  void discardRecv();

  void discardSend();

  void discardConnect();

  struct bufferevent* bev;

  struct evconnlistener* listener;

  RecvRequest* recvRequest;

  SendRequest* sendRequest;

  ConnectRequest* connectRequest;
};


Try<std::shared_ptr<Socket::Impl>> libeventSocket(int s)
{
  struct bufferevent* bev = bufferevent_socket_new(
      ev_base,
      s,
      BEV_OPT_DEFER_CALLBACKS | BEV_OPT_THREADSAFE);
  if (bev == NULL) {
    return Error("Failed to create socket: bufferevent_socket_new");
  }

  return std::make_shared<LibeventSocketImpl>(bev, s);
}


namespace internal {

template <typename Request, typename Value>
void satisfyRequest(Request* request, const Value& value)
{
  assert(request);
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
  } else {
    request->promise.set(value);
  }

  delete request;
}

template <typename Request>
void failRequest(Request* request, const std::string& error)
{
  assert(request);
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
  } else {
    request->promise.fail(error);
  }

  delete request;
}

} // namespace internal {


void LibeventSocketImpl::recvCb(struct bufferevent* bev, void* arg)
{
  bufferevent_disable(bev, EV_READ);
  LibeventSocketImpl* impl = reinterpret_cast<LibeventSocketImpl*>(arg);
  assert(impl);
  RecvRequest* request = impl->recvRequest;
  if (request) {
    impl->recvRequest = NULL;
    internal::satisfyRequest(
        request,
        bufferevent_read(bev, request->data, request->size));
  }
}


void LibeventSocketImpl::sendCb(struct bufferevent* bev, void* arg)
{
  LibeventSocketImpl* impl = reinterpret_cast<LibeventSocketImpl*>(arg);
  assert(impl);
  SendRequest* request = impl->sendRequest;
  if (request) {
    impl->sendRequest = NULL;
    internal::satisfyRequest(request, request->size);
  }
}


void LibeventSocketImpl::eventCb(
    struct bufferevent* bev,
    short events,
    void* arg)
{
  LibeventSocketImpl* impl = reinterpret_cast<LibeventSocketImpl*>(arg);
  assert(impl);
  if (events & BEV_EVENT_EOF) {
    if (impl->recvRequest) {
      RecvRequest* request = impl->recvRequest;
      impl->recvRequest = NULL;
      internal::satisfyRequest(request, 0);
    }
    if (impl->sendRequest) {
      SendRequest* request = impl->sendRequest;
      impl->sendRequest = NULL;
      internal::satisfyRequest(request, 0);
    }
    if (impl->connectRequest) {
      ConnectRequest* request = impl->connectRequest;
      impl->connectRequest = NULL;
      internal::failRequest(request, "Failed connect: connection closed");
    }
  } else if (events & BEV_EVENT_ERROR) {
    if (impl->recvRequest) {
      RecvRequest* request = impl->recvRequest;
      impl->recvRequest = NULL;
      internal::failRequest(request, "Failed recv: connection error");
    }
    if (impl->sendRequest) {
      SendRequest* request = impl->sendRequest;
      impl->sendRequest = NULL;
      internal::failRequest(request, "Failed send: connection error");
    }
    if (impl->connectRequest) {
      ConnectRequest* request = impl->connectRequest;
      impl->connectRequest = NULL;
      internal::failRequest(request, "Failed connect: connection error");
    }
  } else if (events & BEV_EVENT_CONNECTED) {
    CHECK(impl->recvRequest == NULL);
    CHECK(impl->sendRequest == NULL);
    if (impl->connectRequest) {
      ConnectRequest* request = impl->connectRequest;
      impl->connectRequest = NULL;
      internal::satisfyRequest(request, Nothing());
    }
  }
}


LibeventSocketImpl::LibeventSocketImpl(struct bufferevent* _bev, int _s)
  : Socket::Impl(_s),
    bev(_bev),
    listener(NULL),
    recvRequest(NULL),
    sendRequest(NULL),
    connectRequest(NULL) {
  bufferevent_setcb(
      bev,
      &LibeventSocketImpl::recvCb,
      &LibeventSocketImpl::sendCb,
      &LibeventSocketImpl::eventCb,
      this);
}


void LibeventSocketImpl::discardConnect()
{
  bufferevent_lock(bev);

  ConnectRequest *request = connectRequest;
  if (request != NULL) {
    connectRequest = NULL;
    request->promise.discard();
    delete request;
  }

  bufferevent_unlock(bev);
}


Future<Nothing> LibeventSocketImpl::connect(const Node& node)
{
  if (connectRequest != NULL) {
    return Failure("Socket is already connecting");
  }

  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = PF_INET;
  addr.sin_port = htons(node.port);
  addr.sin_addr.s_addr = node.ip;

  connectRequest = new ConnectRequest();
  Future<Nothing> future = connectRequest->promise.future()
    .onDiscard(lambda::bind(&LibeventSocketImpl::discardConnect, this));

  if (bufferevent_socket_connect(
      bev,
      reinterpret_cast<struct sockaddr*>(&addr),
      sizeof(addr)) < 0) {
    return Failure("Failed to create connection: bufferevent_socket_connect");
  }

  return future;
}


void LibeventSocketImpl::discardRecv()
{
  bufferevent_lock(bev);

  RecvRequest *request = recvRequest;
  if (request != NULL) {
    recvRequest = NULL;
    request->promise.discard();
    delete request;
  }

  bufferevent_unlock(bev);
}


Future<size_t> LibeventSocketImpl::recv(char* data, size_t size)
{
  if (recvRequest != NULL) {
    return Failure("Socket is already receiving");
  }

  recvRequest = new RecvRequest(data, size);
  Future<size_t> future = recvRequest->promise.future()
    .onDiscard(lambda::bind(&LibeventSocketImpl::discardRecv, this));

  bufferevent_setwatermark(bev, EV_READ, 0, size);
  bufferevent_enable(bev, EV_READ);

  return future;
}


void LibeventSocketImpl::discardSend()
{
  bufferevent_lock(bev);

  SendRequest *request = sendRequest;
  if (request != NULL) {
    sendRequest = NULL;
    request->promise.discard();
    delete request;
  }

  bufferevent_unlock(bev);
}


Future<size_t> LibeventSocketImpl::send(const char* data, size_t size)
{
  if (sendRequest != NULL) {
    return Failure("Socket is already sending");
  }

  sendRequest = new SendRequest(size);
  Future<size_t> future = sendRequest->promise.future()
    .onDiscard(lambda::bind(&LibeventSocketImpl::discardSend, this));

  bufferevent_write(bev, data, size);

  return future;
}


Future<size_t> LibeventSocketImpl::sendfile(int fd, off_t offset, size_t size)
{
  if (sendRequest != NULL) {
    return Failure("Socket is already sending");
  }

  sendRequest = new SendRequest(size);
  Future<size_t> future = sendRequest->promise.future()
    .onDiscard(lambda::bind(&LibeventSocketImpl::discardSend, this));

  evbuffer_add_file(bufferevent_get_output(bev), fd, offset, size);

  return future;
}


namespace internal {

struct Accept
{
  Promise<Socket> promise;
};

void acceptcb(
    struct evconnlistener* listener,
    int sock,
    struct sockaddr* sa,
    int sa_len,
    void* arg)
{
  evconnlistener_disable(listener);
  struct event_base* ev_base = evconnlistener_get_base(listener);
  struct bufferevent* bev = bufferevent_socket_new(
      ev_base,
      sock,
      BEV_OPT_DEFER_CALLBACKS | BEV_OPT_THREADSAFE);

  Accept* accept = reinterpret_cast<Accept*>(arg);
  Try<Socket> socket = Socket::create(Socket::LIBEVENT, sock);
  if (socket.isError()) {
    accept->promise.fail(socket.error());
  } else {
    accept->promise.set(socket.get());
  }
  delete accept;
}

} // namespace internal {


Try<Nothing> LibeventSocketImpl::listen(int backlog)
{
  if (listener != NULL) {
    return Error("Socket is already listening");
  }

  listener = evconnlistener_new(
      ev_base,
      NULL,
      NULL,
      LEV_OPT_REUSEABLE,
      backlog,
      s);
  if (listener == NULL) {
    return Error("Failed to listen on socket");
  }

  evconnlistener_disable(listener);
  // TODO(jmlvanre): attach an error callback.

  return Nothing();
}


Future<Socket> LibeventSocketImpl::accept()
{
  if (listener == NULL) {
    return Failure("Socket must be listening in order to accept");
  }

  internal::Accept* accept = new internal::Accept();
  Future<Socket> future = accept->promise.future();
  evconnlistener_set_cb(listener, &internal::acceptcb, accept);
  evconnlistener_enable(listener);
  return future;
}

} // namespace network {
} // namespace process {
