#include <unistd.h>

#include <event2/event.h>
#include <event2/thread.h>

#include <process/logging.hpp>

#include "event_loop.hpp"
#include "libevent.hpp"

namespace process {

struct event_base* ev_base = NULL;

void* EventLoop::run(void*)
{
   do {
    int result = event_base_loop(ev_base, EVLOOP_ONCE);
    if (result < 0) {
      LOG(FATAL) << "Failed to run event loop";
    } else if (result == 1) {
      VLOG(1) << "All events handled, continuing event loop";
      continue;
    } else if (event_base_got_break(ev_base)) {
      break;
    } else if (event_base_got_exit(ev_base)) {
      break;
    }
  } while (true);

  return NULL;
}


void EventLoop::initialize()
{
  if (evthread_use_pthreads() < 0) {
    LOG(FATAL) << "Failed to initialize, evthread_use_pthreads";
  }

  // This enables debugging of libevent calls. We can remove this
  // when the implementation settles and after we gain confidence.
  event_enable_debug_mode();

  ev_base = event_base_new();
  if (ev_base == NULL) {
    LOG(FATAL) << "Failed to initialize, event_base_new";
  }
}

} // namespace process {
