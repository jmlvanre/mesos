#include <unistd.h>

#include <event2/event.h>
#include <event2/thread.h>

#include <process/logging.hpp>

#include "event_loop.hpp"
#include "libevent.hpp"
#include "synchronized.hpp"

namespace process {

struct event_base* ev_base = NULL;


struct event* async_event = NULL;
struct timeval async_tv{3600, 0};
static synchronizable(functions) = SYNCHRONIZED_INITIALIZER;
std::queue<lambda::function<void (void)>> async_functions;
__thread bool in_event_loop = false;

void run_in_event_loop(const lambda::function<void (void)>& f)
{
  if (in_event_loop) {
    f();
  } else {
    synchronized (functions) {
      async_functions.push(f);
      event_active(async_event, EV_TIMEOUT, 0);
    }
  }
}


void async_function(int sock, short which, void *arg)
{
  std::queue<lambda::function<void (void)>> q;
  {
    synchronized (functions) {
      std::swap(q, async_functions);
      evtimer_add(async_event, &async_tv);
    }
  }
  while (!q.empty()) {
    q.front()();
    q.pop();
  }
}


void* EventLoop::run(void*)
{
  in_event_loop = true;
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
  in_event_loop = false;

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
  delay->timer = evtimer_new(ev_base, &internal::handle_delay, delay);
  if (delay->timer == NULL) {
    LOG(FATAL) << "Failed to delay, evtimer_new";
  }

  delay->function = function;

  timeval tval{0, 0};
  if (duration > Seconds(0)) {
    tval = duration.timeval();
  }

  evtimer_add(delay->timer, &tval);
}


double EventLoop::time()
{
  timeval tval;

  // Get the cached time if running the event loop, or call
  // gettimeofday() to get the current time. Since a lot of logic in
  // libprocess depends on time math, we want to log fatal rather than
  // cause logic errors if the time fails.
  if (event_base_gettimeofday_cached(ev_base, &tval) < 0) {
    LOG(FATAL) << "Failed to get time, event_base_gettimeofday_cached";
  }

  // Convert the timeval into a doulbe representing seconds.
  Seconds seconds = Seconds(tval.tv_sec) + Microseconds(tval.tv_usec);
  return seconds.value();
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

  async_event = evtimer_new(ev_base, async_function, NULL);
  evtimer_add(async_event, &async_tv);
}

} // namespace process {
