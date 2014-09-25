#ifndef __HTTP_PROXY_HPP__
#define __HTTP_PROXY_HPP__

#include <process/process.hpp>

#include "event_manager_base.hpp"

namespace process {

// Provides a process that manages sending HTTP responses so as to
// satisfy HTTP/1.1 pipelining. Each request should either enqueue a
// response, or ask the proxy to handle a future response. The process
// is responsible for making sure the responses are sent in the same
// order as the requests. Note that we use a 'Socket' in order to keep
// the underyling file descriptor from getting closed while there
// might still be outstanding responses even though the client might
// have closed the connection (see more discussion in
// SocketManger::close and SocketManager::proxy).
class HttpProxy : public Process<HttpProxy>
{
public:
  explicit HttpProxy(const Socket& _socket, internal::EventManager* _ev_man);
  virtual ~HttpProxy();

  // Enqueues the response to be sent once all previously enqueued
  // responses have been processed (e.g., waited for and sent).
  void enqueue(const http::Response& response, const http::Request& request);

  // Enqueues a future to a response that will get waited on (up to
  // some timeout) and then sent once all previously enqueued
  // responses have been processed (e.g., waited for and sent).
  void handle(Future<http::Response>* future, const http::Request& request);

private:
  // Starts "waiting" on the next available future response.
  void next();

  // Invoked once a future response has been satisfied.
  void waited(const Future<http::Response>& future);

  // Demuxes and handles a response.
  bool process(
      const Future<http::Response>& future,
      const http::Request& request);

  // Handles stream (i.e., pipe) based responses.
  void stream(const Future<short>& poll, const http::Request& request);

  Socket socket; // Wrap the socket to keep it from getting closed.

  // EventManager for sending messages.
  internal::EventManager* ev_man;

  // Describes a queue "item" that wraps the future to the response
  // and the original request.
  // The original request contains needed information such as what
  // encodings are acceptable and whether to persist the connection.
  struct Item
  {
    Item(const http::Request& _request, Future<http::Response>* _future)
      : request(_request), future(_future) {}

    ~Item()
    {
      delete future;
    }

    // Helper for cleaning up a response (i.e., closing any open pipes
    // in the event Response::type is PIPE).
    static void cleanup(const http::Response& response)
    {
      if (response.type == http::Response::PIPE) {
        os::close(response.pipe);
      }
    }

    const http::Request request; // Make a copy.
    Future<http::Response>* future;
  };

  std::queue<Item*> items;

  Option<int> pipe; // Current pipe, if streaming.
};

} // namespace process {

#endif // __HTTP_PROXY_HPP__
