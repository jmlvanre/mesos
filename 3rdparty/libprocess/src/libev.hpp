#include <ev.h>

#include <queue>

#include <process/future.hpp>
#include <process/owned.hpp>

#include <stout/lambda.hpp>

#include "synchronized.hpp"

namespace process {

// Event loop.
extern struct ev_loop* loop;

// Asynchronous watcher for interrupting loop to specifically deal
// with IO watchers and functions (via run_in_event_loop).
extern ev_async async_watcher;

// Server watcher for accepting connections.
extern ev_io server_watcher;

// Queue of I/O watchers to be asynchronously added to the event loop
// (protected by 'watchers' below).
// TODO(benh): Replace this queue with functions that we put in
// 'functions' below that perform the ev_io_start themselves.
extern std::queue<ev_io*>* watchers;
extern synchronizable(watchers);

// Queue of functions to be invoked asynchronously within the vent
// loop (protected by 'watchers' above).
extern std::queue<lambda::function<void(void)>>* functions;


// Wrapper around function we want to run in the event loop.
template <typename T>
void _run_in_event_loop(
    const lambda::function<Future<T>(void)>& f,
    const Owned<Promise<T>>& promise)
{
  // Don't bother running the function if the future has been discarded.
  if (promise->future().hasDiscard()) {
    promise->discard();
  } else {
    promise->set(f());
  }
}


// Helper for running a function in the event loop.
template <typename T>
Future<T> run_in_event_loop(const lambda::function<Future<T>(void)>& f)
{
  Owned<Promise<T>> promise(new Promise<T>());

  Future<T> future = promise->future();

  // Enqueue the function.
  synchronized (watchers) {
    functions->push(lambda::bind(&_run_in_event_loop<T>, f, promise));
  }

  // Interrupt the loop.
  ev_async_send(loop, &async_watcher);

  return future;
}

} // namespace process {
