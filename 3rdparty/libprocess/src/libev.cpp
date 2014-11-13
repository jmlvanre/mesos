#include <ev.h>

#include <queue>

#include <stout/lambda.hpp>

#include "libev.hpp"

namespace process {

struct ev_loop* loop = NULL;
ev_async async_watcher;
std::queue<ev_io*>* watchers = new std::queue<ev_io*>();
synchronizable(watchers);
std::queue<lambda::function<void(void)>>* functions =
  new std::queue<lambda::function<void(void)>>();

} // namespace process {
