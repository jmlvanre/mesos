#include <event2/buffer.h>
#include <event2/bufferevent_ssl.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>

#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <process/socket.hpp>

#include <stout/net.hpp>

#include "libevent.hpp"

using std::queue;
using std::string;

static SSL_CTX* SSL_CONTEXT = NULL;

static bool shouldRequireCertificate;

static bool shouldVerifyCertificate;

struct CRYPTO_dynlock_value
{
  pthread_mutex_t mutex;
};

pthread_mutex_t* ssl_mutexes = NULL;

void locking_function(int mode, int n, const char */*file*/, int /*line*/)
{
  if (mode & CRYPTO_LOCK) {
    pthread_mutex_lock(&ssl_mutexes[n]);
  } else {
    pthread_mutex_unlock(&ssl_mutexes[n]);
  }
}

unsigned long id_function()
{
  return ((unsigned long)pthread_self());
}

struct CRYPTO_dynlock_value *dyn_create_function(
  const char */*file*/, int /*line*/)
{
  struct CRYPTO_dynlock_value *value =
  (struct CRYPTO_dynlock_value *)
  malloc(sizeof(struct CRYPTO_dynlock_value));
  if (!value) {
    return NULL;
  }
  pthread_mutex_init(&value->mutex, NULL);

  return value;
}

void dyn_lock_function(
  int mode, struct CRYPTO_dynlock_value *l, const char */*file*/, int /*line*/)
{
  if (mode & CRYPTO_LOCK) {
    pthread_mutex_lock(&l->mutex);
  } else {
    pthread_mutex_unlock(&l->mutex);
  }
}

void dyn_destroy_function(
  struct CRYPTO_dynlock_value *l, const char */*file*/, int /*line*/)
{
  pthread_mutex_destroy(&l->mutex);
  free(l);
}


namespace process {
namespace network {

class LibeventSSLSocketImpl : public Socket::Impl
{
public:
  LibeventSSLSocketImpl(int _s, struct bufferevent* _bev);

  virtual ~LibeventSSLSocketImpl()
  {
    if (bev) {
      SSL *ctx = bufferevent_openssl_get_ssl(bev);
      SSL_set_shutdown(ctx, SSL_RECEIVED_SHUTDOWN);
      SSL_shutdown(ctx);
      bufferevent_free(bev);
      if (freeSSLCtx) {
        SSL_free(ctx);
      }
    }
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

  virtual void shutdown();

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

  struct AcceptRequest
  {
    AcceptRequest(LibeventSSLSocketImpl* _impl) : impl(_impl) {}
    LibeventSSLSocketImpl* impl;
    Promise<Socket> promise;
    struct bufferevent* bev;
    int sock;
  };

  void _recv(Socket* socket, size_t size);

  void _send(Socket* socket, const char* data, size_t size);

  void _sendfile(Socket* socket, int fd, off_t offset, size_t size);

  void _accept(Socket* socket);

  static void recvCb(struct bufferevent* bev, void* arg);

  static void sendCb(struct bufferevent* bev, void* arg);

  static void eventCb(struct bufferevent* bev, short events, void* arg);

  void discardRecv();

  void discardSend();

  void discardConnect();

  static void acceptEventCb(struct bufferevent* bev, short events, void* arg);

  void doAccept(
    int sock,
    struct sockaddr* sa,
    int sa_len);

  static void acceptCb(
    struct evconnlistener* listener,
    int sock,
    struct sockaddr* sa,
    int sa_len,
    void* arg);

  struct bufferevent* bev;

  struct evconnlistener* listener;

  RecvRequest* recvRequest;

  SendRequest* sendRequest;

  ConnectRequest* connectRequest;

  AcceptRequest* acceptRequest;

  struct PendingAccept {
    int sock;
    struct sockaddr* sa;
    int sa_len;
    void* arg;
  };

  queue<PendingAccept> pendingAccepts;

  bool freeSSLCtx;

  Option<string> peerHostname;
};


namespace internal {

int verify_callback(int ok, X509_STORE_CTX* store)
{
  if (ok != 1) {
    constexpr int bufferSize = 256;
    char buffer[bufferSize];
    X509* cert = X509_STORE_CTX_get_current_cert(store);
    int depth = X509_STORE_CTX_get_error_depth(store);
    int err = X509_STORE_CTX_get_error(store);

    LOG(WARNING) << "Error with certificate at depth: " << stringify(depth);
    X509_NAME_oneline(X509_get_issuer_name(cert), buffer, bufferSize);
    LOG(WARNING) << "Issuer: " << stringify(buffer);
    X509_NAME_oneline(X509_get_subject_name(cert), buffer, bufferSize);
    LOG(WARNING) << "Subject: " << stringify(buffer);
    LOG(WARNING) << "Error(" << stringify(err) << "): "
                 << stringify(X509_verify_cert_error_string(err));
  }

  return ok;
}


Option<string> getEnvironmentPath(const std::string& name)
{
  string result = os::getenv(name, false);

  if(result.empty()) {
    return None();
  }

  if (!os::exists(result)) {
    VLOG(2) << "Path: '" << result << "' not found";
    return None();
  }

  return result;
}


bool getEnvironmentBoolFlag(const string& name, bool defaultValue = false)
{
  const string value = os::getenv(name, false);
  if (!value.empty()) {
    if (value == "1") {
      return true;
    } else if (value == "0") {
      return false;
    } else {
      VLOG(2) << "Boolean flag is expected to be either '0' or '1'";
    }
  }

  return defaultValue;
}


Try<Nothing> postVerify(
    SSL* ssl,
    bool requirePeerCertificate,
    const Option<string>& hostname)
{
  // TODO(nnielsen): We _can_ do post verification of peer hostname
  // and hostname announced in X509.
  X509 *peer = SSL_get_peer_certificate(ssl);
  if (peer) {
    if (SSL_get_verify_result(ssl) != X509_V_OK) {
      return Error("Could not verify peer certificate");
    }

    if (hostname.isNone()) {
      if (requirePeerCertificate) {
        return Error("Cannot verify peer certificate: peer hostname unknown");
      }
    } else {
      int extcount = X509_get_ext_count(peer);
      if (extcount <= 0) {
        return Error("X509_get_ext_count failed: " + stringify(extcount));
      }

      bool hostnameVerified = false;

      for (int i = 0; i < extcount; i++) {
        X509_EXTENSION* ext = X509_get_ext(peer, i);
        const string extstr =
          OBJ_nid2sn(OBJ_obj2nid(X509_EXTENSION_get_object(ext)));

        if (extstr == "subjectAltName") {
#if OPENSSL_VERSION_NUMBER <= 0x00909000L
          X509V3_EXT_METHOD* meth = X509V3_EXT_get(ext);
#else
          const X509V3_EXT_METHOD* meth = X509V3_EXT_get(ext);
#endif
          if (!meth) {
            break;
          }

          const unsigned char* data = ext->value->data;
          STACK_OF(CONF_VALUE) *val = meth->i2v(
              meth,
              meth->d2i(NULL, &data, ext->value->length),
              NULL);

          for (int j = 0; j < sk_CONF_VALUE_num(val); j++) {
            CONF_VALUE* nval = sk_CONF_VALUE_value(val, j);
            const string& nvalName = nval->name;
            const string& nvalValue = nval->value;
            if ((nvalName == "DNS") && (nvalValue == hostname.get())) {
              hostnameVerified = true;
              break;
            }

            LOG(INFO) << "nvalName: " << nvalName
                      << " nvalValue: " << nvalValue;
          }
        }

        if (hostnameVerified) {
          break;
        }
      }

      char data[256];
      X509_NAME* subj = X509_get_subject_name(peer);
      if (!hostnameVerified &&
          subj &&
          X509_NAME_get_text_by_NID(subj, NID_commonName, data, 256) > 0) {
        data[255] = 0;

        if (hostname.get() != data) {
          return Error(
              "Presented CN: " + stringify(data) +
              " does not match peer name: " + hostname.get());
        }

        hostnameVerified = true;
      }

      if (!hostnameVerified) {
        return Error(
            "Could not post-verify presented certificate with hostname " +
            hostname.get());
      }
    }
  } else if (requirePeerCertificate) {
    return Error("Peer did not provide certificate");
  }

  return Nothing();
}


string SSLError(int code)
{
  // SSL library guarantees to stay within 120 bytes.
  char errorBuffer[128];

  ERR_error_string(code, errorBuffer);
  string ret(errorBuffer);

  if (code == SSL_ERROR_SYSCALL) {
    ret += SSLError(ERR_get_error());
  }

  return ret;
}

} // namespace internal {


Try<std::shared_ptr<Socket::Impl>> libeventSSLSocket(int s, void* arg)
{
  static volatile bool initialized = false;
  static volatile bool initializing = true;
  // Try and do the initialization or wait for it to complete.
  if (initialized && !initializing) {
    // Already initialized. Do nothing.
  } else if (initialized && initializing) {
    while (initializing);
    // Wait for initialization to complete.
  } else {
    if (!__sync_bool_compare_and_swap(&initialized, false, true)) {
      while (initializing);
      // Wait for initialization to complete.
    } else {
      // Initialize.
      /* We MUST have entropy, or else there's no point to crypto. */
      if (!RAND_poll()) {
        LOG(FATAL) << "SSL socket requires entropy";
      }

      // Initialize the OpenSSL library.
      SSL_load_error_strings();
      SSL_library_init();
      // Install SSL threading call backs.

      // Prepare mutexes for threading call backs.
      if (ssl_mutexes == NULL) {
        ssl_mutexes = new pthread_mutex_t[CRYPTO_num_locks()];
        for (int i = 0; i < CRYPTO_num_locks(); i++) {
          pthread_mutex_init(&ssl_mutexes[i], NULL);
        }
      }

      CRYPTO_set_id_callback(id_function);
      CRYPTO_set_locking_callback(locking_function);
      CRYPTO_set_dynlock_create_callback(dyn_create_function);
      CRYPTO_set_dynlock_lock_callback(dyn_lock_function);
      CRYPTO_set_dynlock_destroy_callback(dyn_destroy_function);

      SSL_CONTEXT = SSL_CTX_new(SSLv23_method());

      // Disable SSL session caching.
      SSL_CTX_set_session_cache_mode(SSL_CONTEXT, SSL_SESS_CACHE_OFF);

      // Set a session id to avoid connection termination upon
      // re-connect. We can use something more relevant when we care
      // about session caching.
      const uint64_t session_ctx = 7;
      const unsigned char* session_id =
      reinterpret_cast<const unsigned char*>(&session_ctx);
      if (SSL_CTX_set_session_id_context(
          SSL_CONTEXT,
          session_id,
          sizeof(session_ctx)) <= 0) {
        LOG(FATAL) << "Session id context size exceeds maximum";
      }

      const Option<string> key = internal::getEnvironmentPath("SSL_KEY");
      if (key.isNone()) {
        LOG(FATAL) << "SSL requires key! NOTE: Set path with SSL_KEY";
      }

      const Option<string> certificate =
        internal::getEnvironmentPath("SSL_CERT");
      if (certificate.isNone()) {
        LOG(FATAL) << "SSL requires certificate! NOTE: Set path with SSL_CERT";
      }

      const Option<string> CAFilePath =
        internal::getEnvironmentPath("SSL_CA_FILE");
      if (CAFilePath.isNone()) {
        VLOG(2) << "NOTE: Set CA file path with CA_FILE";
      }

      const Option<string> CADirectoryPath =
        internal::getEnvironmentPath("SSL_CA_DIR");
      if (CADirectoryPath.isNone()) {
        VLOG(2) << "NOTE: Set CA directory path with CA_DIR";
      }

      shouldVerifyCertificate =
        internal::getEnvironmentBoolFlag("SSL_VERIFY_CERT");
      if (!shouldVerifyCertificate) {
        VLOG(2) << "Will not verify peer certificate!";
        VLOG(2) << "NOTE: Set SSL_VERIFY_CERT=1 to enable.";
      }

      shouldRequireCertificate = internal::getEnvironmentBoolFlag(
        "SSL_REQUIRE_CERT");
      if (!shouldRequireCertificate) {
        VLOG(2) << "Will only verify peer certificate if presented!";
        VLOG(2) << "NOTE: To require peer certificate, "
                << "set SSL_REQUIRE_CERT=1 to enable";
      }

      if (!shouldVerifyCertificate) {
        if (shouldRequireCertificate) {
          // Requiring a certificate implies that is should be verified.
          shouldVerifyCertificate = true;
        }
      }

      string acceptedCiphers = os::getenv("SSL_CIPHERS", false);
      if (acceptedCiphers.empty()) {
        // TLSv1 ciphers chosen baseed on Amazon's security policy:
        // http://docs.aws.amazon.com/ElasticLoadBalancing/latest/
        // DeveloperGuide/elb-security-policy-table.html
        acceptedCiphers =
          "AES128-SHA:AES256-SHA:RC4-SHA:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA:"
          "DHE-RSA-AES256-SHA:DHE-DSS-AES256-SHA";

        VLOG(2) << "Using default ciphers: " << acceptedCiphers;
      }

      if (shouldVerifyCertificate) {
        // Set CA locations.
        if (CAFilePath.isSome() || CADirectoryPath.isSome()) {
          const char* CAFile = CAFilePath.isSome()
                                ? CAFilePath.get().c_str()
                                : NULL;

          const char* CADir = CADirectoryPath.isSome()
                              ? CADirectoryPath.get().c_str()
                              : NULL;

          if (SSL_CTX_load_verify_locations(SSL_CONTEXT, CAFile, CADir) <= 0) {
            int err = ERR_get_error();
            LOG(FATAL) << "Could not load CA file and/or directory(" +
                stringify(err)  + "): " + internal::SSLError(err) + " -> " +
                CAFile;
          }

          if (CAFile) {
            VLOG(2) << "Using CA file: " << CAFile;
          }
        }

        // Set SSL peer verification callback.
        int verify = SSL_VERIFY_PEER;
        if (shouldRequireCertificate) {
          verify |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
        }

        SSL_CTX_set_verify(SSL_CONTEXT, verify, internal::verify_callback);
        if (SSL_CTX_set_default_verify_paths(SSL_CONTEXT) <= 0) {
          LOG(FATAL) << "Could not load default CA file and/or directory";
        }

        const int verificationDepth = 4;

        SSL_CTX_set_verify_depth(SSL_CONTEXT, verificationDepth);
      }

      // Set certificate chain.
      if (SSL_CTX_use_certificate_chain_file(
          SSL_CONTEXT,
          certificate.get().c_str()) <= 0) {
        LOG(FATAL) << "Could not load cert file";
      }

      // Set pirvate key.
      if (SSL_CTX_use_PrivateKey_file(
          SSL_CONTEXT,
          key.get().c_str(),
          SSL_FILETYPE_PEM) <= 0) {
        LOG(FATAL) << "Could not load key file";
      }

      // Validate key.
      if (!SSL_CTX_check_private_key(SSL_CONTEXT)) {
        LOG(FATAL) << "Private key does not match the certificate public key";
      }

      // Set ciphers.
      if (SSL_CTX_set_cipher_list(SSL_CONTEXT, acceptedCiphers.c_str()) == 0) {
        LOG(FATAL) << "Could not set ciphers: " + acceptedCiphers;
      }

      // Disable SSLv2.
      SSL_CTX_set_options(SSL_CONTEXT, SSL_OP_NO_SSLv2);

      // Finished initializing.
      initializing = false;
    }
  }

  return std::make_shared<LibeventSSLSocketImpl>(
      s,
      reinterpret_cast<struct bufferevent*>(arg));
}


namespace internal {

template <typename Request, typename Value>
void satisfyRequest(Request* request, const Value& value)
{
  assert(request != NULL);
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
  } else {
    request->promise.set(value);
  }

  delete request;
}

template <typename Request>
void failRequest(Request* request, const string& error)
{
  assert(request != NULL);
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
  } else {
    request->promise.fail(error);
  }

  delete request;
}

} // namespace internal {


void LibeventSSLSocketImpl::shutdown()
{
  assert(bev != NULL);
  bufferevent_lock(bev);

  if (recvRequest) {
    RecvRequest* request = recvRequest;
    recvRequest = NULL;
    internal::satisfyRequest(request, 0);
  }

  bufferevent_unlock(bev);
}


void LibeventSSLSocketImpl::recvCb(struct bufferevent* bev, void* arg)
{
  LibeventSSLSocketImpl* impl = reinterpret_cast<LibeventSSLSocketImpl*>(arg);
  assert(impl != NULL);
  RecvRequest* request = impl->recvRequest;
  if (request) {
    bufferevent_disable(bev, EV_READ);
    impl->recvRequest = NULL;
    internal::satisfyRequest(
        request,
        bufferevent_read(bev, request->data, request->size));
  }
}


void LibeventSSLSocketImpl::sendCb(struct bufferevent* bev, void* arg)
{
  struct evbuffer *output = bufferevent_get_output(bev);
  LibeventSSLSocketImpl* impl = reinterpret_cast<LibeventSSLSocketImpl*>(arg);
  assert(impl != NULL);
  SendRequest* request = impl->sendRequest;
  if (request) {
    impl->sendRequest = NULL;
    internal::satisfyRequest(request, request->size);
  }
}


void LibeventSSLSocketImpl::eventCb(
    struct bufferevent* bev,
    short events,
    void* arg)
{
  LibeventSSLSocketImpl* impl = reinterpret_cast<LibeventSSLSocketImpl*>(arg);
  assert(impl != NULL);
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
    VLOG(1) << "Socket error: "
            << stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
    if (impl->recvRequest) {
      RecvRequest* request = impl->recvRequest;
      impl->recvRequest = NULL;
      internal::failRequest(request, "Failed recv, connection error: " +
          stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())));
    }
    if (impl->sendRequest) {
      SendRequest* request = impl->sendRequest;
      impl->sendRequest = NULL;
      internal::failRequest(request, "Failed send, connection error: " +
          stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())));
    }
    if (impl->connectRequest) {
      ConnectRequest* request = impl->connectRequest;
      impl->connectRequest = NULL;
      internal::failRequest(request, "Failed connect, connection error: " +
          stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())));
    }
  } else if (events & BEV_EVENT_CONNECTED) {
    CHECK(impl->recvRequest == NULL);
    CHECK(impl->sendRequest == NULL);
    if (impl->connectRequest) {
      ConnectRequest* request = impl->connectRequest;
      impl->connectRequest = NULL;

      assert(impl->bev != NULL);

      // Do post-validation of connection.
      if (shouldVerifyCertificate) {
        SSL *ssl = bufferevent_openssl_get_ssl(impl->bev);

        Try<Nothing> verify = internal::postVerify(
            ssl,
            shouldRequireCertificate,
            impl->peerHostname);
        if (verify.isError()) {
          VLOG(1) << "Failed connect, post verification error: "
                  << verify.error();
          internal::failRequest(request, verify.error());
          return;
        }
      }

      internal::satisfyRequest(request, Nothing());
    }
  }
}


LibeventSSLSocketImpl::LibeventSSLSocketImpl(int _s, struct bufferevent* _bev)
  : Socket::Impl(_s),
    bev(_bev),
    listener(NULL),
    recvRequest(NULL),
    sendRequest(NULL),
    connectRequest(NULL),
    acceptRequest(NULL),
    freeSSLCtx(_bev != NULL) {
  if (bev) {
    bufferevent_setcb(
        bev,
        LibeventSSLSocketImpl::recvCb,
        LibeventSSLSocketImpl::sendCb,
        LibeventSSLSocketImpl::eventCb,
        this);
  }
}


void LibeventSSLSocketImpl::discardConnect()
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


Future<Nothing> LibeventSSLSocketImpl::connect(const Node& node)
{
  if (connectRequest != NULL) {
    return Failure("Socket is already connecting");
  }

  if (node.ip == 0) {
    const Try<string> hostname = net::hostname();
    if (hostname.isSome()) {
      peerHostname = hostname.get();
    }
  } else {
    const Try<string> hostname = net::getHostname(node.ip);
    if (hostname.isError()) {
      VLOG(2) << "Could not determine hostname of peer";
    } else {
      VLOG(2) << "Connecting to " << hostname.get();
      peerHostname = hostname.get();
    }
  }

  assert(bev == NULL);

  SSL* ssl = SSL_new(SSL_CONTEXT);
  if (ssl == NULL) {
    return Failure("Failed to connect: SSL_new");
  }

  bev = bufferevent_openssl_socket_new(
      ev_base,
      -1,
      ssl,
      BUFFEREVENT_SSL_CONNECTING,
      BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);

  if (bev == NULL) {
    return Failure(
      "Failed to connect: bufferevent_openssl_socket_new");
  }

  bufferevent_setcb(
      bev,
      &LibeventSSLSocketImpl::recvCb,
      &LibeventSSLSocketImpl::sendCb,
      &LibeventSSLSocketImpl::eventCb,
      this);

  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = PF_INET;
  addr.sin_port = htons(node.port);
  addr.sin_addr.s_addr = node.ip;

  connectRequest = new ConnectRequest();
  Future<Nothing> future = connectRequest->promise.future()
    .onDiscard(lambda::bind(&LibeventSSLSocketImpl::discardConnect, this));

  if (bufferevent_socket_connect(
      bev,
      reinterpret_cast<struct sockaddr*>(&addr),
      sizeof(addr)) < 0) {
    return Failure("Failed to connect: bufferevent_socket_connect");
  }

  return future;
}


void LibeventSSLSocketImpl::discardRecv()
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


void LibeventSSLSocketImpl::_recv(Socket* socket, size_t size)
{
  if (recvRequest != NULL) {
    bufferevent_setwatermark(bev, EV_READ, 0, size);
    bufferevent_enable(bev, EV_READ);
  }

  delete socket;
}


Future<size_t> LibeventSSLSocketImpl::recv(char* data, size_t size)
{
  if (recvRequest != NULL) {
    return Failure("Socket is already receiving");
  }

  assert(bev != NULL);
  recvRequest = new RecvRequest(data, size);
  Future<size_t> future = recvRequest->promise.future()
    .onDiscard(lambda::bind(&LibeventSSLSocketImpl::discardRecv, this));

  // Copy this socket into 'run_in_event_loop' to keep the it alive.
  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_recv,
      this,
      new Socket(socket()),
      size));

  return future;
}


void LibeventSSLSocketImpl::discardSend()
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


void LibeventSSLSocketImpl::_send(Socket* socket, const char* data, size_t size)
{
  if (sendRequest != NULL) {
    bufferevent_write(bev, data, size);
  }

  delete socket;
}


Future<size_t> LibeventSSLSocketImpl::send(const char* data, size_t size)
{
  if (sendRequest != NULL) {
    return Failure("Socket is already sending");
  }

  assert(bev != NULL);
  sendRequest = new SendRequest(size);
  Future<size_t> future = sendRequest->promise.future()
    .onDiscard(lambda::bind(&LibeventSSLSocketImpl::discardSend, this));

  // Copy this socket into 'run_in_event_loop' to keep the it alive.
  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_send,
      this,
      new Socket(socket()),
      data,
      size));

  return future;
}


void LibeventSSLSocketImpl::_sendfile(
    Socket* socket,
    int fd,
    off_t offset,
    size_t size)
{
  if (sendRequest != NULL) {
    evbuffer_add_file(bufferevent_get_output(bev), fd, offset, size);
  }

  delete socket;
}


Future<size_t> LibeventSSLSocketImpl::sendfile(
    int fd,
    off_t offset,
    size_t size)
{
  if (sendRequest != NULL) {
    return Failure("Socket is already sending");
  }

  assert(bev != NULL);
  sendRequest = new SendRequest(size);
  Future<size_t> future = sendRequest->promise.future()
    .onDiscard(lambda::bind(&LibeventSSLSocketImpl::discardSend, this));

  // Copy this socket into 'run_in_event_loop' to keep the it alive.
  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_sendfile,
      this,
      new Socket(socket()),
      fd,
      offset,
      size));

  return future;
}


void LibeventSSLSocketImpl::acceptEventCb(
    struct bufferevent* bev,
    short events,
    void* arg)
{
  AcceptRequest* request = reinterpret_cast<AcceptRequest*>(arg);
  assert(request != NULL);

  if (events & BEV_EVENT_CONNECTED) {
    // Do post verification of provided certificates.
    if (shouldVerifyCertificate) {
      SSL *ssl = bufferevent_openssl_get_ssl(bev);

      Try<Nothing> verify = internal::postVerify(
          ssl,
          shouldRequireCertificate,
          request->impl->peerHostname);
      if (verify.isError()) {
        VLOG(1) << "Failed accept, post verification error: " + verify.error();
        request->promise.fail(verify.error());
        delete request;
        return;
      }
    }

    Try<Socket> socket =
      Socket::create(Socket::SSL, request->sock, request->bev);

    if (socket.isError()) {
      request->promise.fail(socket.error());
    } else {
      request->promise.set(socket.get());
    }
  } else if (events & BEV_EVENT_ERROR) {
    VLOG(1) << "Socket error: "
            << stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
    request->promise.fail("Error connecting accepted socket: " +
        stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())));
  } else {
    request->promise.fail("Error connecting accepted socket");
  }

  delete request;
}


void LibeventSSLSocketImpl::doAccept(
    int sock,
    struct sockaddr* sa,
    int sa_len)
{
  AcceptRequest* request = acceptRequest;
  CHECK(request);

  SSL* client_ssl = SSL_new(SSL_CONTEXT);
  if (client_ssl == NULL) {
    request->promise.fail("Accept failed, SSL_new");
    delete request;
    return;
  }

  evconnlistener_disable(listener);

  Try<string> hostname =
    net::getHostname(reinterpret_cast<sockaddr_in*>(sa)->sin_addr.s_addr);
  if (hostname.isError()) {
    VLOG(2) << "Could not determine hostname of peer";
  } else {
    VLOG(2) << "Accepting from " << hostname.get();
    peerHostname = hostname.get();
  }

  struct event_base* ev_base = evconnlistener_get_base(listener);

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

  acceptRequest = NULL;

  request->bev = bev;
  request->sock = sock;

  bufferevent_setcb(
      bev,
      NULL,
      NULL,
      LibeventSSLSocketImpl::acceptEventCb,
      request);
}


void LibeventSSLSocketImpl::acceptCb(
    struct evconnlistener* listener,
    int sock,
    struct sockaddr* sa,
    int sa_len,
    void* arg)
{
  LibeventSSLSocketImpl* impl = reinterpret_cast<LibeventSSLSocketImpl*>(arg);
  assert(impl != NULL);
  AcceptRequest* request = impl->acceptRequest;

  // Since the bufferevent_ssl implementation uses a bufferevent
  // underneath, we can not perfectly control the listening socket.
  // This means we sometimes get called back in 'acceptCb' multiple
  // times even though we're only interested in 1 at a time. We queue
  // the remaining accept ready sockets on the 'pendingAccepts' queue.
  if (request) {
    impl->doAccept(sock, sa, sa_len);
  } else {
    impl->pendingAccepts.push(PendingAccept{sock, sa, sa_len, arg});
  }
}


Try<Nothing> LibeventSSLSocketImpl::listen(int backlog)
{
  if (listener != NULL) {
    return Error("Socket is already listening");
  }

  assert(bev == NULL);

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


void LibeventSSLSocketImpl::_accept(Socket* socket)
{
  if (pendingAccepts.empty()) {
    evconnlistener_set_cb(listener, &LibeventSSLSocketImpl::acceptCb, this);
    evconnlistener_enable(listener);
  } else {
    const PendingAccept &p = pendingAccepts.front();
    doAccept(p.sock, p.sa, p.sa_len);
    pendingAccepts.pop();
  }

  delete socket;
}


Future<Socket> LibeventSSLSocketImpl::accept()
{
  if (listener == NULL) {
    return Failure("Socket must be listening in order to accept");
  }

  if (acceptRequest != NULL) {
    return Failure("Socket is already accepting");
  }

  acceptRequest = new AcceptRequest(this);
  Future<Socket> future = acceptRequest->promise.future();

  // Copy this socket into 'run_in_event_loop' to keep the it alive.
  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_accept,
      this,
      new Socket(socket())));

  return future;
}

} // namespace network {
} // namespace process {
