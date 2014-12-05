#ifndef __LIBEVENT_HPP__
#define __LIBEVENT_HPP__

#include <stout/thread.hpp>

namespace process {

// Event loop.
extern struct event_base* base;

// Per thread bool pointer. The extra level of indirection from
// _in_event_loop_ to __in_event_loop__ is used in order to take
// advantage of the ThreadLocal operators without needing the extra
// dereference as well as lazily construct the actual bool.
extern ThreadLocal<bool>* _in_event_loop_;

#define __in_event_loop__ *(*_in_event_loop_ == NULL ?               \
  *_in_event_loop_ = new bool(false) : *_in_event_loop_)

void run_in_event_loop(const lambda::function<void(void)>& f);

} // namespace process {

#endif // __LIBEVENT_HPP__
