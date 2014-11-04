#ifndef __PROCESS_CLOCK_HPP__
#define __PROCESS_CLOCK_HPP__

#include <process/time.hpp>

#include <stout/duration.hpp>
#include <stout/lambda.hpp>
#include <stout/nothing.hpp>

namespace process {

// Forward declarations (to avoid circular dependencies).
class ProcessBase;
class Time;
class Timer;

class Clock
{
public:
  static Time now();
  static Time now(ProcessBase* process);

  static Timer timer(
      const Duration& duration,
      const lambda::function<void(void)>& thunk);

  static bool cancel(const Timer& timer);

  static void pause();
  static bool paused();

  static void resume();

  static void advance(const Duration& duration);
  static void advance(ProcessBase* process, const Duration& duration);

  static void update(const Time& time);
  static void update(ProcessBase* process, const Time& time);

  static void order(ProcessBase* from, ProcessBase* to);

  // When the clock is paused this returns only after
  //   (1) all expired timers are executed,
  //   (2) no processes are running, and
  //   (3) no processes are ready to run.
  //
  // In other words, this function blocks synchronously until no other
  // execution on any processes will occur unless the clock is
  // advanced.
  //
  // TODO(benh): Move this function elsewhere, for instance, to a
  // top-level function in the 'process' namespace since it deals with
  // both processes and the clock.
  static void settle();

  // When the clock is paused this returns true if all timers that
  // expire before the paused time have executed, otherwise false.
  // Note that if the clock continually gets advanced concurrently
  // this function may never return true because the "paused" time
  // will continue to get pushed out farther in the future making more
  // timers candidates for execution.
  static bool settled();
};

} // namespace process {

#endif // __PROCESS_CLOCK_HPP__
