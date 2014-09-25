#ifndef __EVENT_MANAGER_HPP__
#define __EVENT_MANAGER_HPP__

#include "event_manager_base.hpp"

namespace process {

class EventManager : public internal::EventManager
{
public:
  virtual ~EventManager() {}

protected:
  EventManager() {}

};

} // namespace process {

#endif // __EVENT_MANAGER_HPP__
