#include <event2/buffer.h>
#include <event2/bufferevent_ssl.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <process/queue.hpp>
#include <process/socket.hpp>

#include <stout/net.hpp>

#include "libevent.hpp"
#include "synchronized.hpp"
#include "openssl.hpp"

// Locking:
// We use the BEV_OPT_THREADSAFE flag when constructing bufferevents
// so that all bufferevent specific functions run from within the
// event loop will have the lock on the bufferevent already acquired.
// This means that everywhere else we need to manually call
// 'bufferevent_lock(bev)' and 'bufferevent_unlock(bev)'; However, due
// to a deadlock scneario in libevent-openssl (v 2.0.21) we currently
// modify bufferevents using continuations in the event loop. See
// 'Continuation' comment below.

// Continuation:
// There is a deadlock scenario in libevent-openssl (v 2.0.21) when
// modifying the bufferevent (bev) from another thread (not the event
// loop). To avoid this we run all bufferevent manipulation logic in
// continuations that are executed within the event loop.

// Connection Extra FD:
// In libevent-openssl (v 2.0.21) we've had issues using the
// 'bufferevent_openssl_socket_new' call with the CONNECTING state and
// an existing socket. Therefore we allow it to construct its own
// fd and clean it up along with the Impl object when the bev is
// freed using the BEV_OPT_CLOSE_ON_FREE option.

using std::queue;
using std::string;

namespace process {
namespace network {

class LibeventSSLSocketImpl : public Socket::Impl
{
public:
  LibeventSSLSocketImpl(int _s);

  virtual ~LibeventSSLSocketImpl()
  {
    // We defer termination and destruction of all event loop specific
    // calls and structures to the eventDtor function run in the event
    // loop.
    run_in_event_loop(lambda::bind(
        &LibeventSSLSocketImpl::eventDtor,
        eventLoopHandle,
        bev,
        listener,
        freeSSLCtx));
  }

  virtual Future<Nothing> connect(const Address& address);

  virtual Future<size_t> recv(char* data, size_t size);

  virtual Future<size_t> send(const char* data, size_t size);

  virtual Future<size_t> sendfile(int fd, off_t offset, size_t size);

  virtual Try<Nothing> listen(int backlog);

  virtual Future<Socket> accept();

  // This call is used to do the equivalent of shutting down the read
  // end. This means finishing the future of any outstanding read
  // request.
  virtual void shutdown();

  // We need a post-initializer because 'shared_from_this()' is not
  // valid until the constructor has finished.
  void postInit()
  {
    eventLoopHandle =
      new std::weak_ptr<LibeventSSLSocketImpl>(shared<LibeventSSLSocketImpl>());
  }

private:
  // A helper class that transitions an accepted socket to an ssl
  // connected socket. With the libevent-openssl library, once we
  // return from the 'acceptCallback()' which is scheduled by 'listen'
  // then we still need to wait for the 'BEV_EVENT_CONNECTED' state
  // before we know the SSL connection has been established.
  class AcceptHelper
  {
  public:
    struct AcceptRequest
    {
      AcceptRequest() : bev(NULL), sock(-1), sa_len(0) {
        memset(&sa, 0, sizeof(sa));
      }
      Promise<Socket> promise;
      struct bufferevent* bev;
      int sock;
      struct sockaddr sa;
      int sa_len;
    };

    // This is called when the equivalent of 'accept' returns. The
    // role of this function is to set up the SSL object and bev.
    static void acceptCallback(
        AcceptRequest* request,
        struct evconnlistener* listener,
        int sock,
        struct sockaddr* sa,
        int sa_len);

    // This is the continuation of 'acceptCallback' that handles error
    // states or 'BEV_EVENT_CONNECTED' events and satisfies the
    // promise by constructing a new socket if the connection was
    // successfuly established.
    static void acceptEventCallback(
        struct bufferevent* bev,
        short events,
        void* arg);
  };

  struct RecvRequest
  {
    RecvRequest(char* _data, size_t _size)
      : data(_data), size(_size) {}
    Promise<size_t> promise;
    char* data;
    size_t size;
  };

  struct SendRequest
  {
    SendRequest(size_t _size)
      : size(_size) {}
    Promise<size_t> promise;
    size_t size;
  };

  struct ConnectRequest
  {
    ConnectRequest() {}
    Promise<Nothing> promise;
  };

  // This is a private constructor used by the AcceptHelper.
  LibeventSSLSocketImpl(
      int _s,
      struct bufferevent* bev,
      Option<std::string>&& peerHostname);

  // The following are continuations of their respective names that
  // run in the event loop.
  void _shutdown();
  void _recv(const std::shared_ptr<LibeventSSLSocketImpl>& socket, size_t size);
  void _send(
      const std::shared_ptr<LibeventSSLSocketImpl>& socket,
      const char* data,
      size_t size);
  void _sendfile(
      const std::shared_ptr<LibeventSSLSocketImpl>& socket,
      int fd,
      off_t offset,
      size_t size);

  static void acceptCallback(
      struct evconnlistener* listener,
      int sock,
      struct sockaddr* sa,
      int sa_len,
      void* arg);

  // The following are function pairs of static functions to member
  // functions. The static functions test and hold the weak pointer to
  // the socket before calling the member functions. This protects
  // against the socket being destroyed before the event-loop calls
  // the callbacks.
  static void recvCallback(struct bufferevent* bev, void* arg);
  void recvCallback();

  static void sendCallback(struct bufferevent* bev, void* arg);
  void sendCallback();

  static void eventCallback(struct bufferevent* bev, short events, void* arg);
  void eventCallback(short events);

  static void discardRecv(
      const std::weak_ptr<LibeventSSLSocketImpl>& weak_socket);
  void _discardRecv(const std::shared_ptr<LibeventSSLSocketImpl>& socket);

  // This function is responsible for cleaning up all structures
  // required by the event-loop. This is a safety against the socket
  // being destroyed before event-loop calls are invoked requiring
  // valid data structures.
  static void eventDtor(
      std::weak_ptr<LibeventSSLSocketImpl>* eventLoopHandle,
      struct bufferevent* bev,
      struct evconnlistener* listener,
      bool freeSSLCtx);

  struct bufferevent* bev;

  struct evconnlistener* listener;

  // Protects the following instance variables.
  synchronizable(this);
  Owned<RecvRequest> recvRequest;
  Owned<SendRequest> sendRequest;
  Owned<ConnectRequest> connectRequest;

  // This is a weak pointer to the current socket impl that is used
  // by the event loop to test whether the socket is still valid. It
  // is the responsibility of the event-loop through the 'eventDtor'
  // to clean up this pointer.
  std::weak_ptr<LibeventSSLSocketImpl>* eventLoopHandle;

  // This queue stores buffered accepted sockets. 'Queue' is a thread
  // safe queue implementation, and the event-loop pushes connected
  // sockets onto it, the 'accept()' call pops them off. We wrap these
  // sockets with futures so that we can pass errors through and chain
  // futures as well.
  Queue<Future<Socket>> acceptQueue;

  bool freeSSLCtx;

  Option<string> peerHostname;
};


Try<std::shared_ptr<Socket::Impl>> libeventSSLSocket(int s)
{
  openssl::initialize();
  std::shared_ptr<LibeventSSLSocketImpl> socket =
    std::make_shared<LibeventSSLSocketImpl>(s);
  socket->postInit();
  return socket;
}


// Only runs in event-loop. It is a continuation of 'shutdown'.
void LibeventSSLSocketImpl::_shutdown()
{
  CHECK(__in_event_loop__);

  CHECK_NOTNULL(bev);

  bufferevent_lock(bev);
  { // Bev locking scope.
    Owned<RecvRequest> request;

    // Swap the recvRequest under the object lock.
    synchronized (this) {
      std::swap(request, recvRequest);
    }

    // If there is still a pending receive request then close it.
    if (request.get() != NULL) {
      request->promise.set(bufferevent_read(bev, request->data, request->size));
    }
  } // End bev locking scope.
  bufferevent_unlock(bev);
}


void LibeventSSLSocketImpl::shutdown()
{
  run_in_event_loop(lambda::bind(&LibeventSSLSocketImpl::_shutdown, this));
}


// Only runs in event-loop. No locks required. See 'Locking' note at
// top of file.
void LibeventSSLSocketImpl::recvCallback(struct bufferevent* bev, void* arg)
{
  CHECK(__in_event_loop__);

  std::weak_ptr<LibeventSSLSocketImpl>* handle =
    reinterpret_cast<std::weak_ptr<LibeventSSLSocketImpl>*>(CHECK_NOTNULL(arg));

  std::shared_ptr<LibeventSSLSocketImpl> impl(handle->lock());

  // Don't call the 'recvCallback' unless the socket is still valid.
  if (impl) {
    impl->recvCallback();
  }
}


// Only runs in event-loop. Member function continuation of static
// recvCallback.
void LibeventSSLSocketImpl::recvCallback()
{
  CHECK(__in_event_loop__);

  Owned<RecvRequest> request;

  synchronized (this) {
    std::swap(request, recvRequest);
  }

  if (request.get() != NULL) {
    bufferevent_disable(bev, EV_READ);
    request->promise.set(bufferevent_read(bev, request->data, request->size));
  }
}


// Only runs in event-loop. No locks required. See 'Locking' note at
// top of file.
void LibeventSSLSocketImpl::sendCallback(struct bufferevent* bev, void* arg)
{
  CHECK(__in_event_loop__);

  std::weak_ptr<LibeventSSLSocketImpl>* handle =
    reinterpret_cast<std::weak_ptr<LibeventSSLSocketImpl>*>(CHECK_NOTNULL(arg));

  std::shared_ptr<LibeventSSLSocketImpl> impl(handle->lock());

  // Don't call the 'sendCallback' unless the socket is still valid.
  if (impl) {
    impl->sendCallback();
  }
}


// Only runs in event-loop. Member function continuation of static
// recvCallback.
void LibeventSSLSocketImpl::sendCallback()
{
  CHECK(__in_event_loop__);

  Owned<SendRequest> request;

  synchronized(this) {
    std::swap(request, sendRequest);
  }

  if (request.get() != NULL) {
    request->promise.set(request->size);
  }
}


// Only runs in event-loop. No locks required. See 'Locking' note at
// top of file.
void LibeventSSLSocketImpl::eventCallback(
    struct bufferevent* bev,
    short events,
    void* arg)
{
  CHECK(__in_event_loop__);

  std::weak_ptr<LibeventSSLSocketImpl>* handle =
    reinterpret_cast<std::weak_ptr<LibeventSSLSocketImpl>*>(CHECK_NOTNULL(arg));

  std::shared_ptr<LibeventSSLSocketImpl> impl(handle->lock());

  // Don't call the 'eventCallback' unless the socket is still valid.
  if (impl != NULL) {
    impl->eventCallback(events);
  }
}


// Only runs in event-loop. Member function continuation of static
// recvCallback.
void LibeventSSLSocketImpl::eventCallback(short events)
{
  CHECK(__in_event_loop__);

  Owned<RecvRequest> currentRecvRequest;
  Owned<SendRequest> currentSendRequest;
  Owned<ConnectRequest> currentConnectRequest;
  // In all of the following conditions, we're interested in swapping
  // the value of the requests with null (if they are already null,
  // then there's no harm).
  if (events & BEV_EVENT_EOF ||
      events & BEV_EVENT_CONNECTED ||
      events & BEV_EVENT_ERROR) {
    synchronized (this) {
      std::swap(currentRecvRequest, recvRequest);
      std::swap(currentSendRequest, sendRequest);
      std::swap(currentConnectRequest, connectRequest);
    }
  }

  // If a request below is null, then no such request is in progress,
  // either because it was never created, it has already been
  // completed, or it has been discarded.

  if (events & BEV_EVENT_EOF) {
    // At end of file, close the connection.
    if (currentRecvRequest.get() != NULL) {
      currentRecvRequest->promise.set(0);
    }
    if (currentSendRequest.get() != NULL) {
      currentSendRequest->promise.set(0);
    }
    if (currentConnectRequest.get() != NULL) {
      currentConnectRequest->promise.fail("Failed connect: connection closed");
    }
  } else if (events & BEV_EVENT_CONNECTED) {
    // We should not have receiving or sending request while still
    // connecting.
    CHECK(currentRecvRequest.get() == NULL);
    CHECK(currentSendRequest.get() == NULL);
    CHECK(currentConnectRequest.get() != NULL);
    // If we're connecting, then we've succeeded. Time to do
    // post-verification.
    CHECK_NOTNULL(bev);

    // Do post-validation of connection.
    SSL* ssl = bufferevent_openssl_get_ssl(bev);

    Try<Nothing> verify = openssl::verify(ssl, peerHostname);

    if (verify.isError()) {
      VLOG(1) << "Failed connect, post verification error: "
              << verify.error();
      currentConnectRequest->promise.fail(verify.error());
      return;
    }

    currentConnectRequest->promise.set(Nothing());
  } else if (events & BEV_EVENT_ERROR) {
    std::ostringstream errorStream;
    if (EVUTIL_SOCKET_ERROR() != 0) {
      errorStream << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    } else {
      unsigned long opensslError = bufferevent_get_openssl_error(bev);
      char errorBuffer[1024];
      memset(errorBuffer, 0, 1024);
      ERR_error_string_n(opensslError, errorBuffer, 1023);
      errorStream << errorBuffer;
    }
    // If there is a valid error, fail any requests and log the error.
    VLOG(1) << "Socket error: "
            << errorStream.str();
    if (currentRecvRequest.get() != NULL) {
      currentRecvRequest->promise.fail(
          "Failed recv, connection error: " +
          errorStream.str());
    }
    if (currentSendRequest.get() != NULL) {
      currentSendRequest->promise.fail(
          "Failed send, connection error: " +
          errorStream.str());
    }
    if (currentConnectRequest.get() != NULL) {
      currentConnectRequest->promise.fail(
          "Failed connect, connection error: " +
          errorStream.str());
    }
  }
}


// For the connecting socket we currently don't use the fd associated
// with 'Socket'. See the 'Connection Extra FD' note at top of file.
LibeventSSLSocketImpl::LibeventSSLSocketImpl(int _s)
  : Socket::Impl(_s),
    bev(NULL),
    listener(NULL),
    recvRequest(NULL),
    sendRequest(NULL),
    connectRequest(NULL),
    eventLoopHandle(NULL),
    freeSSLCtx(false)
{
  synchronizer(this) = SYNCHRONIZED_INITIALIZER;
}


// For the connecting socket we currently don't use the fd associated
// with 'Socket'. See the 'Connection Extra FD' note at top of file.
LibeventSSLSocketImpl::LibeventSSLSocketImpl(
    int _s,
    struct bufferevent* _bev,
    Option<std::string>&& _peerHostname)
  : Socket::Impl(_s),
    bev(_bev),
    listener(NULL),
    recvRequest(NULL),
    sendRequest(NULL),
    connectRequest(NULL),
    eventLoopHandle(NULL),
    freeSSLCtx(true),
    peerHostname(std::move(_peerHostname))
{
  synchronizer(this) = SYNCHRONIZED_INITIALIZER;
}


// Only runs in event-loop.
// Once this function is called, it should not be possible for more
// event loop callbacks to be triggered with the given bev. This is
// important because we delete eventLoopHandle which is the callback
// argument for any eventloop callbacks. This function is responsible
// for ensuring the bev is disabled, and cleaning up any remaining
// state associated with the event loop.
void LibeventSSLSocketImpl::eventDtor(
    std::weak_ptr<LibeventSSLSocketImpl>* eventLoopHandle,
    struct bufferevent* bev,
    struct evconnlistener* listener,
    bool freeSSLCtx)
{
  CHECK(__in_event_loop__);

  if (listener != NULL) {
    evconnlistener_free(listener);
  }

  if (bev != NULL) {
    SSL* ssl = bufferevent_openssl_get_ssl(bev);
    // Workaround for SSL shutdown, see http://www.wangafu.net/~nickm/libevent-book/Ref6a_advanced_bufferevents.html // NOLINT
    SSL_set_shutdown(ssl, SSL_RECEIVED_SHUTDOWN);
    SSL_shutdown(ssl);
    bufferevent_disable(bev, EV_READ | EV_WRITE);

    // Since we are using a separate fd for the connecting socket we
    // end up using BEV_OPT_CLOSE_ON_FREE for the connecting, but
    // not for the accepting side. since the BEV_OPT_CLOSE_ON_FREE
    // also frees the SSL object, we need to manually free it for
    // the accepting case. See the 'Connection Extra FD' note at top
    // of file.
    if (freeSSLCtx) {
      SSL_free(ssl);
    }

    // For the connecting socket BEV_OPT_CLOSE_ON_FREE will close
    // the fd. See note below.
    bufferevent_free(bev);
  }

  delete eventLoopHandle;
}


Future<Nothing> LibeventSSLSocketImpl::connect(const Address& address)
{
  if (connectRequest.get() != NULL) {
    return Failure("Socket is already connecting");
  }

  if (address.ip == 0) {
    // Set the local hostname on the socket for peer validation.
    const Try<string> hostname = net::hostname();
    if (hostname.isSome()) {
      peerHostname = hostname.get();
    }
  } else {
    // If the connecting address is not local, then set the remote
    // hostname for peer validation.
    const Try<string> hostname = net::getHostname(address.ip);
    if (hostname.isError()) {
      VLOG(2) << "Could not determine hostname of peer";
    } else {
      VLOG(2) << "Connecting to " << hostname.get();
      peerHostname = hostname.get();
    }
  }

  CHECK(bev == NULL);

  SSL* ssl = SSL_new(openssl::context());
  if (ssl == NULL) {
    return Failure("Failed to connect: SSL_new");
  }

  // Construct the bufferevent in the connecting state. We don't use
  // the existing FD due to an issue in libevent-openssl. See the
  // 'Connection Extra FD' note at top of file.
  bev = bufferevent_openssl_socket_new(
      base,
      -1,
      ssl,
      BUFFEREVENT_SSL_CONNECTING,
      BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);

  if (bev == NULL) {
    return Failure(
      "Failed to connect: bufferevent_openssl_socket_new");
  }

  // Assign the callbacks for the bufferevent.
  bufferevent_setcb(
      bev,
      &LibeventSSLSocketImpl::recvCallback,
      &LibeventSSLSocketImpl::sendCallback,
      &LibeventSSLSocketImpl::eventCallback,
      eventLoopHandle);

  // TODO(jmlvanre): sync on new IP address setup.
  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = PF_INET;
  addr.sin_port = htons(address.port);
  addr.sin_addr.s_addr = address.ip;


  // Optimistically construct a 'connectRequest' and future.
  Owned<ConnectRequest> request(new ConnectRequest());
  Future<Nothing> future = request->promise.future();

  // Assign 'connectRequest' under lock, fail on error.
  synchronized (this) {
    if (connectRequest.get() != NULL) {
      return Failure("Socket is already connecting");
    } else {
      std::swap(request, connectRequest);
    }
  }

  if (bufferevent_socket_connect(
      bev,
      reinterpret_cast<struct sockaddr*>(&addr),
      sizeof(addr)) < 0) {
    return Failure("Failed to connect: bufferevent_socket_connect");
  }

  return future;
}


Future<size_t> LibeventSSLSocketImpl::recv(char* data, size_t size)
{
  // Optimistically construct a 'RecvRequest' and future.
  Owned<RecvRequest> request(new RecvRequest(data, size));
  std::weak_ptr<LibeventSSLSocketImpl> socket(shared<LibeventSSLSocketImpl>());

  Future<size_t> future = request->promise.future()
    .onDiscard(lambda::bind(
        &LibeventSSLSocketImpl::discardRecv,
        socket));

  // Assign 'recvRequest' under lock, fail on error.
  synchronized (this) {
    if (recvRequest.get() != NULL) {
      return Failure("Socket is already receiving");
    } else {
      std::swap(request, recvRequest);
    }
  }

  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_recv,
      this,
      shared<LibeventSSLSocketImpl>(),
      size));

  return future;
}


// Only runs in event-loop. It is a continuation of 'recv'.
void LibeventSSLSocketImpl::_recv(
    const std::shared_ptr<LibeventSSLSocketImpl>& socket,
    size_t size)
{
  CHECK(__in_event_loop__);

  bool recv = false;

  synchronized (this) {
    recv = recvRequest.get() != NULL;
  }

  if (recv) {
    bufferevent_setwatermark(bev, EV_READ, 0, size);
    bufferevent_enable(bev, EV_READ);
  }
}


void LibeventSSLSocketImpl::discardRecv(
    const std::weak_ptr<LibeventSSLSocketImpl>& weak_socket)
{
  std::shared_ptr<LibeventSSLSocketImpl> socket(weak_socket.lock());

  if (socket != NULL) {
    run_in_event_loop(lambda::bind(
        &LibeventSSLSocketImpl::_discardRecv,
        socket.get(),
        socket));
  }
}


// Only runs in event-loop.
void LibeventSSLSocketImpl::_discardRecv(
    const std::shared_ptr<LibeventSSLSocketImpl>& socket)
{
  CHECK(__in_event_loop__);

  Owned<RecvRequest> request;

  synchronized (this) {
    std::swap(request, recvRequest);
  }

  // Discard the promise outside of the object lock as the callbacks
  // can be expensive.
  if (request.get() != NULL) {
    bufferevent_disable(bev, EV_READ);
    request->promise.discard();
  }
}


Future<size_t> LibeventSSLSocketImpl::send(const char* data, size_t size)
{
  // Optimistically construct a 'SendRequest' and future.
  Owned<SendRequest> request(new SendRequest(size));
  Future<size_t> future = request->promise.future();

  // Assign 'sendRequest' under lock, fail on error.
  synchronized (this) {
    if (sendRequest.get() != NULL) {
      return Failure("Socket is already sending");
    } else {
      std::swap(request, sendRequest);
    }
  }

  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_send,
      this,
      shared<LibeventSSLSocketImpl>(),
      data,
      size));

  return future;
}


// Only runs in event-loop.
void LibeventSSLSocketImpl::_send(
    const std::shared_ptr<LibeventSSLSocketImpl>& socket,
    const char* data,
    size_t size)
{
  CHECK(__in_event_loop__);

  bool send = false;

  synchronized (this) {
    send = sendRequest.get() != NULL;
  }

  if (send) {
    bufferevent_write(bev, data, size);
  }
}


Future<size_t> LibeventSSLSocketImpl::sendfile(
    int fd,
    off_t offset,
    size_t size)
{
  // Optimistically construct a 'SendRequest' and future.
  Owned<SendRequest> request(new SendRequest(size));
  Future<size_t> future = request->promise.future();

  // Assign 'sendRequest' under lock, fail on error.
  synchronized (this) {
    if (sendRequest.get() != NULL) {
      return Failure("Socket is already sending");
    } else {
      std::swap(request, sendRequest);
    }
  }

  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_sendfile,
      this,
      shared<LibeventSSLSocketImpl>(),
      fd,
      offset,
      size));

  return future;
}


// Only runs in event-loop.
void LibeventSSLSocketImpl::_sendfile(
    const std::shared_ptr<LibeventSSLSocketImpl>& socket,
    int fd,
    off_t offset,
    size_t size)
{
  CHECK(__in_event_loop__);

  bool sendfile = false;

  synchronized (this) {
    sendfile = sendRequest.get() != NULL;
  }

  if (sendfile) {
    evbuffer_add_file(bufferevent_get_output(bev), fd, offset, size);
  }
}


Try<Nothing> LibeventSSLSocketImpl::listen(int backlog)
{
  if (listener != NULL) {
    return Error("Socket is already listening");
  }

  CHECK(bev == NULL);

  listener = evconnlistener_new(
      base,
      &LibeventSSLSocketImpl::acceptCallback,
      eventLoopHandle,
      LEV_OPT_REUSEABLE,
      backlog,
      s);

  if (listener == NULL) {
    return Error("Failed to listen on socket");
  }

  // TODO(jmlvanre): attach an error callback.

  return Nothing();
}


Future<Socket> LibeventSSLSocketImpl::accept()
{
  return acceptQueue.get().then([](const Future<Socket>& future) {
    return future;
  });
}


// Only runs in event-loop.
void LibeventSSLSocketImpl::acceptCallback(
    struct evconnlistener* listener,
    int sock,
    struct sockaddr* sa,
    int sa_len,
    void* arg)
{
  CHECK(__in_event_loop__);

  std::weak_ptr<LibeventSSLSocketImpl>* handle =
  reinterpret_cast<std::weak_ptr<LibeventSSLSocketImpl>*>(CHECK_NOTNULL(arg));

  std::shared_ptr<LibeventSSLSocketImpl> impl(handle->lock());

  if (impl) {
    AcceptHelper::AcceptRequest* request = new AcceptHelper::AcceptRequest();
    impl->acceptQueue.put(request->promise.future());
    AcceptHelper::acceptCallback(request, listener, sock, sa, sa_len);
  }
}


// Only runs in event-loop.
void LibeventSSLSocketImpl::AcceptHelper::acceptCallback(
    AcceptRequest* request,
    struct evconnlistener* listener,
    int sock,
    struct sockaddr* sa,
    int sa_len)
{
  CHECK(__in_event_loop__);

  // Set up SSL object.
  SSL* client_ssl = SSL_new(openssl::context());
  if (client_ssl == NULL) {
    request->promise.fail("Accept failed, SSL_new");
    delete request;
    return;
  }

  struct event_base* ev_base = evconnlistener_get_base(listener);

  // Construct the bufferevent in the accepting state.
  struct bufferevent* bev = bufferevent_openssl_socket_new(
      ev_base,
      sock,
      client_ssl,
      BUFFEREVENT_SSL_ACCEPTING,
      BEV_OPT_THREADSAFE);
  if (bev == NULL) {
    request->promise.fail("Accept failed: bufferevent_openssl_socket_new");
    SSL_free(client_ssl);
    delete request;
    return;
  }

  request->bev = bev;
  request->sock = sock;
  // We need to copy the sockaddr because the backing pointer of 'sa'
  // is not guaranteed to stick around past this function scope.
  memcpy(&request->sa, sa, sa_len);
  request->sa_len = sa_len;

  bufferevent_setcb(
      bev,
      NULL,
      NULL,
      &LibeventSSLSocketImpl::AcceptHelper::acceptEventCallback,
      request);
}


// Only runs in event-loop.
void LibeventSSLSocketImpl::AcceptHelper::acceptEventCallback(
    struct bufferevent* bev,
    short events,
    void* arg)
{
  CHECK(__in_event_loop__);

  AcceptHelper::AcceptRequest* request =
    reinterpret_cast<AcceptHelper::AcceptRequest*>(CHECK_NOTNULL(arg));
  if (events & BEV_EVENT_EOF) {
    request->promise.fail("Failed accept: connection closed");
  } else if (events & BEV_EVENT_CONNECTED) {
    // We will receive a 'CONNECTED' state on an accepting socket
    // once the connection is established. Time to do
    // post-verification.
    SSL* ssl = bufferevent_openssl_get_ssl(bev);
    CHECK_NOTNULL(ssl);

    Try<string> hostname =
      net::getHostname(
          reinterpret_cast<sockaddr_in*>(&request->sa)->sin_addr.s_addr);
    Option<string> peerHostname = None();

    if (hostname.isError()) {
      VLOG(2) << "Could not determine hostname of peer";
    } else {
      VLOG(2) << "Accepting from " << hostname.get();
      peerHostname = hostname.get();
    }

    Try<Nothing> verify = openssl::verify(ssl, peerHostname);

    if (verify.isError()) {
      VLOG(1) << "Failed accept, post verification error: "
              << verify.error();
      request->promise.fail(verify.error());
      delete request;
      return;
    }

    std::shared_ptr<LibeventSSLSocketImpl> socketImpl =
      std::shared_ptr<LibeventSSLSocketImpl>(new LibeventSSLSocketImpl(
          request->sock, bev,
          std::move(peerHostname)));
    socketImpl->postInit();
    Socket socket = Socket::Impl::socket(socketImpl);

    bufferevent_setcb(
        bev,
        LibeventSSLSocketImpl::recvCallback,
        LibeventSSLSocketImpl::sendCallback,
        LibeventSSLSocketImpl::eventCallback,
        socketImpl->eventLoopHandle);

    request->promise.set(socket);
  } else if (events & BEV_EVENT_ERROR) {
    std::ostringstream errorStream;
    if (EVUTIL_SOCKET_ERROR() != 0) {
      errorStream << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    } else {
      unsigned long opensslError = bufferevent_get_openssl_error(bev);
      char errorBuffer[1024];
      memset(errorBuffer, 0, 1024);
      ERR_error_string_n(opensslError, errorBuffer, 1023);
      errorStream << errorBuffer;
    }
    // Fail the accept request and log the error.
    VLOG(1) << "Socket error: "
            << errorStream.str();
    request->promise.fail(
        "Failed accept: connection error: " + errorStream.str());
  }

  delete request;
}

} // namespace network {
} // namespace process {
