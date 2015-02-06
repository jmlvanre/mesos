#include <unistd.h>

#include <event2/event.h>
#include <event2/thread.h>

#include <process/logging.hpp>

#include "event_loop.hpp"
#include "libevent.hpp"
#include "synchronized.hpp"

namespace process {

struct event_base* base = NULL;

static synchronizable(functions) = SYNCHRONIZED_INITIALIZER;
std::queue<lambda::function<void(void)>> async_functions;

ThreadLocal<bool>* _in_event_loop_ = new ThreadLocal<bool>();


void async_function(int sock, short which, void *arg)
{
  struct event* ev = reinterpret_cast<struct event*>(arg);
  event_free(ev);

  std::queue<lambda::function<void(void)>> q;
  {
    synchronized (functions) {
      std::swap(q, async_functions);
    }
  }
  while (!q.empty()) {
    q.front()();
    q.pop();
  }
}


void run_in_event_loop(const lambda::function<void(void)>& f)
{
  if (__in_event_loop__) {
    f();
  } else {
    synchronized (functions) {
      async_functions.push(f);
      // Add an event and activate it to interrupt the event loop.
      // TODO(jmlvanre): after libevent v 2.1 we can use
      // event_self_cbarg instead of re-assigning the event. For now
      // we manually re-assign the event to pass in the pointer to the
      // event itself as the callback argument.
      struct event* ev = evtimer_new(base, async_function, NULL);
      event_assign(ev, base, -1, 0, async_function, ev);
      event_active(ev, EV_TIMEOUT, 0);
    }
  }
}


void* EventLoop::run(void*)
{
  __in_event_loop__ = true;

  do {
    int result = event_base_loop(base, EVLOOP_ONCE);
    if (result < 0) {
      LOG(FATAL) << "Failed to run event loop";
    } else if (result == 1) {
      VLOG(1) << "All events handled, continuing event loop";
      continue;
    } else if (event_base_got_break(base)) {
      break;
    } else if (event_base_got_exit(base)) {
      break;
    }
  } while (true);

  __in_event_loop__ = false;

  return NULL;
}


namespace internal {

struct Delay
{
  void(*function)(void);
  event* timer;
};

void handle_delay(int, short, void* arg)
{
  Delay* delay = reinterpret_cast<Delay*>(arg);
  delay->function();
  delete delay;
}

}  // namespace internal {


void EventLoop::delay(const Duration& duration, void(*function)(void))
{
  internal::Delay* delay = new internal::Delay();
  delay->timer = evtimer_new(base, &internal::handle_delay, delay);
  if (delay->timer == NULL) {
    LOG(FATAL) << "Failed to delay, evtimer_new";
  }

  delay->function = function;

  timeval t{0, 0};
  if (duration > Seconds(0)) {
    t = duration.timeval();
  }

  evtimer_add(delay->timer, &t);
}


double EventLoop::time()
{
  // Get the cached time if running the event loop, or call
  // gettimeofday() to get the current time. Since a lot of logic in
  // libprocess depends on time math, we want to log fatal rather than
  // cause logic errors if the time fails.
  timeval t;
  if (event_base_gettimeofday_cached(base, &t) < 0) {
    LOG(FATAL) << "Failed to get time, event_base_gettimeofday_cached";
  }

  return Duration(t).secs();
}


void EventLoop::initialize()
{
  if (evthread_use_pthreads() < 0) {
    LOG(FATAL) << "Failed to initialize, evthread_use_pthreads";
  }

  // This enables debugging of libevent calls. We can remove this
  // when the implementation settles and after we gain confidence.
  event_enable_debug_mode();

  base = event_base_new();
  if (base == NULL) {
    LOG(FATAL) << "Failed to initialize, event_base_new";
  }
}

} // namespace process {
