#include <process/defer.hpp>

#include <process/io.hpp>

#include "http_proxy.hpp"

using process::http::InternalServerError;
using process::http::NotFound;
using process::http::Request;
using process::http::Response;
using process::http::ServiceUnavailable;

using std::string;
using std::stringstream;

namespace process {

HttpProxy::HttpProxy(const Socket& _socket, internal::EventManager* _ev_man)
  : ProcessBase(ID::generate("__http__")),
    socket(_socket),
    ev_man(_ev_man) {}


HttpProxy::~HttpProxy()
{
  // Need to make sure response producers know not to continue to
  // create a response (streaming or otherwise).
  if (pipe.isSome()) {
    os::close(pipe.get());
  }
  pipe = None();

  while (!items.empty()) {
    Item* item = items.front();

    // Attempt to discard the future.
    item->future->discard();

    // But it might have already been ready. In general, we need to
    // wait until this future is potentially ready in order to attempt
    // to close a pipe if one exists.
    item->future->onReady(lambda::bind(&Item::cleanup, lambda::_1));

    items.pop();
    delete item;
  }
}


void HttpProxy::enqueue(const Response& response, const Request& request)
{
  handle(new Future<Response>(response), request);
}


void HttpProxy::handle(Future<Response>* future, const Request& request)
{
  items.push(new Item(request, future));

  if (items.size() == 1) {
    next();
  }
}


void HttpProxy::next()
{
  if (items.size() > 0) {
    // Wait for any transition of the future.
    items.front()->future->onAny(
        defer(self(), &HttpProxy::waited, lambda::_1));
  }
}


void HttpProxy::waited(const Future<Response>& future)
{
  CHECK(items.size() > 0);
  Item* item = items.front();

  CHECK(future == *item->future);

  // Process the item and determine if we're done or not (so we know
  // whether to start waiting on the next responses).
  bool processed = process(*item->future, item->request);

  items.pop();
  delete item;

  if (processed) {
    next();
  }
}


bool HttpProxy::process(const Future<Response>& future, const Request& request)
{
  if (!future.isReady()) {
    // TODO(benh): Consider handling other "states" of future
    // (discarded, failed, etc) with different HTTP statuses.
    ev_man->send(ServiceUnavailable(), request, socket);
    return true; // All done, can process next response.
  }

  Response response = future.get();

  // If the response specifies a path, try and perform a sendfile.
  if (response.type == Response::PATH) {
    // Make sure no body is sent (this is really an error and
    // should be reported and no response sent.
    response.body.clear();

    const string& path = response.path;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
      if (errno == ENOENT || errno == ENOTDIR) {
          VLOG(1) << "Returning '404 Not Found' for path '" << path << "'";
          ev_man->send(NotFound(), request, socket);
      } else {
        const char* error = strerror(errno);
        VLOG(1) << "Failed to send file at '" << path << "': " << error;
        ev_man->send(InternalServerError(), request, socket);
      }
    } else {
      struct stat s; // Need 'struct' because of function named 'stat'.
      if (fstat(fd, &s) != 0) {
        const char* error = strerror(errno);
        VLOG(1) << "Failed to send file at '" << path << "': " << error;
        ev_man->send(InternalServerError(), request, socket);
      } else if (S_ISDIR(s.st_mode)) {
        VLOG(1) << "Returning '404 Not Found' for directory '" << path << "'";
        ev_man->send(NotFound(), request, socket);
      } else {
        // While the user is expected to properly set a 'Content-Type'
        // header, we fill in (or overwrite) 'Content-Length' header.
        stringstream out;
        out << s.st_size;
        response.headers["Content-Length"] = out.str();

        if (s.st_size == 0) {
          ev_man->send(response, request, socket);
          return true; // All done, can process next request.
        }

        VLOG(1) << "Sending file at '" << path << "' with length " << s.st_size;

        // TODO(benh): Consider a way to have the socket manager turn
        // on TCP_CORK for both sends and then turn it off.
        ev_man->send(
            new HttpResponseEncoder(socket, response, request),
            true);

        // Note the file descriptor gets closed by FileEncoder.
        ev_man->send(
            new FileEncoder(socket, fd, s.st_size),
            request.keepAlive);
      }
    }
  } else if (response.type == Response::PIPE) {
    // Make sure no body is sent (this is really an error and
    // should be reported and no response sent.
    response.body.clear();

    // Make sure the pipe is nonblocking.
    Try<Nothing> nonblock = os::nonblock(response.pipe);
    if (nonblock.isError()) {
      const char* error = strerror(errno);
      VLOG(1) << "Failed make pipe nonblocking: " << error;
      ev_man->send(InternalServerError(), request, socket);
      return true; // All done, can process next response.
    }

    // While the user is expected to properly set a 'Content-Type'
    // header, we fill in (or overwrite) 'Transfer-Encoding' header.
    response.headers["Transfer-Encoding"] = "chunked";

    VLOG(1) << "Starting \"chunked\" streaming";

    ev_man->send(
        new HttpResponseEncoder(socket, response, request),
        true);

    pipe = response.pipe;

    io::poll(pipe.get(), io::READ).onAny(
        defer(self(), &Self::stream, lambda::_1, request));

    return false; // Streaming, don't process next response (yet)!
  } else {
    ev_man->send(response, request, socket);
  }

  return true; // All done, can process next response.
}


void HttpProxy::stream(const Future<short>& poll, const Request& request)
{
  // TODO(benh): Use 'splice' on Linux.

  CHECK(pipe.isSome());

  bool finished = false; // Whether we're done streaming.

  if (poll.isReady()) {
    // Read and write.
    CHECK(poll.get() == io::READ);
    const size_t size = 4 * 1024; // 4K.
    char data[size];
    while (!finished) {
      ssize_t length = ::read(pipe.get(), data, size);
      if (length < 0 && (errno == EINTR)) {
        // Interrupted, try again now.
        continue;
      } else if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        // Might block, try again later.
        io::poll(pipe.get(), io::READ).onAny(
            defer(self(), &Self::stream, lambda::_1, request));
        break;
      } else {
        std::ostringstream out;
        if (length <= 0) {
          // Error or closed, treat both as closed.
          if (length < 0) {
            // Error.
            const char* error = strerror(errno);
            VLOG(1) << "Read error while streaming: " << error;
          }
          out << "0\r\n" << "\r\n";
          finished = true;
        } else {
          // Data!
          out << std::hex << length << "\r\n";
          out.write(data, length);
          out << "\r\n";
        }

        // We always persist the connection when we're not finished
        // streaming.
        ev_man->send(
            new DataEncoder(socket, out.str()),
            finished ? request.keepAlive : true);
      }
    }
  } else if (poll.isFailed()) {
    VLOG(1) << "Failed to poll: " << poll.failure();
    ev_man->send(InternalServerError(), request, socket);
    finished = true;
  } else {
    VLOG(1) << "Unexpected discarded future while polling";
    ev_man->send(InternalServerError(), request, socket);
    finished = true;
  }

  if (finished) {
    os::close(pipe.get());
    pipe = None();
    next();
  }
}

} // namespace process {