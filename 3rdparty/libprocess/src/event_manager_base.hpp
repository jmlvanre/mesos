#ifndef __EVENT_MANAGER_BASE_HPP__
#define __EVENT_MANAGER_BASE_HPP__

#include "encoder.hpp"

namespace process {

namespace internal {

class EventManager
{
public:
  virtual ~EventManager() {}

  virtual void send(Encoder* encoder, bool persist) = 0;

  virtual void send(
      const http::Response& response,
      const http::Request& request,
      const Socket& socket) = 0;

protected:
  EventManager() {}

};

}  // namespace internal {

} // namespace process {

#endif // __EVENT_MANAGER_BASE_HPP__
