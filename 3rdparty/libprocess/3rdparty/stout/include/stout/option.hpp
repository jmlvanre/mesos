/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __STOUT_OPTION_HPP__
#define __STOUT_OPTION_HPP__

#include <assert.h>

#include <algorithm>

#include <stout/none.hpp>
#include <stout/some.hpp>

template <typename T>
class Option
{
public:
  static Option<T> none()
  {
    return Option<T>();
  }

  static Option<T> some(const T& t)
  {
    return Option<T>(t);
  }

  Option() : t(NULL) {}

  Option(const T& _t) { t = new (storage) T(_t); }

  Option(T&& _t) { t = new (storage) T(std::move(_t)); }

  template <typename U>
  Option(const U& u) { t = new (storage) T(u); }

  Option(const None& none) : t(NULL) {}

  template <typename U>
  Option(const _Some<U>& some) { t = new (storage) T(some.t); }

  Option(const Option<T>& that)
  {
    t = that.t ? new (storage) T(*that.t) : NULL;
  }

  Option(Option<T>&& that)
  {
    if (that.t) {
      t = new (storage) T(std::move(*that.t));
      that.reset();
    } else {
      t = NULL;
    }
  }

  ~Option() { reset(); }

  Option<T>& operator = (const Option<T>& that)
  {
    if (this != &that) {
      reset();
      t = that.t ? new (storage) T(*that.t) : NULL;
    }
    return *this;
  }

  bool operator == (const Option<T>& that) const
  {
    return (isNone() && that.isNone()) ||
           (isSome() && that.isSome() && *t == *that.t);
  }

  bool operator != (const Option<T>& that) const
  {
    return !operator == (that);
  }

  bool operator == (const T& that) const
  {
    return isSome() && *t == that;
  }

  bool operator != (const T& that) const
  {
    return !operator == (that);
  }

  bool isSome() const { return t != NULL; }
  bool isNone() const { return t == NULL; }

  const T& get() const { assert(isSome()); return *t; }

  // This must return a copy to avoid returning a reference to a temporary.
  T get(const T& _t) const { return isNone() ? _t : *t; }

  void reset() {
    if (t != NULL) {
      t->~T();
      t = NULL;
    }
  }

private:
  T* t;
#if __cplusplus >= 201103L
  alignas(T) char storage[sizeof(T)];
#else
  char storage[sizeof(T)];
#endif // __cplusplus >= 201103L
};


template <typename T>
Option<T> min(const Option<T>& left, const Option<T>& right)
{
  if (left.isSome() && right.isSome()) {
    return std::min(left.get(), right.get());
  } else if (left.isSome()) {
    return left.get();
  } else if (right.isSome()) {
    return right.get();
  } else {
    return Option<T>::none();
  }
}


template <typename T>
Option<T> min(const Option<T>& left, const T& right)
{
  return min(left, Option<T>(right));
}


template <typename T>
Option<T> min(const T& left, const Option<T>& right)
{
  return min(Option<T>(left), right);
}


template <typename T>
Option<T> max(const Option<T>& left, const Option<T>& right)
{
  if (left.isSome() && right.isSome()) {
    return std::max(left.get(), right.get());
  } else if (left.isSome()) {
    return left.get();
  } else if (right.isSome()) {
    return right.get();
  } else {
    return Option<T>::none();
  }
}


template <typename T>
Option<T> max(const Option<T>& left, const T& right)
{
  return max(left, Option<T>(right));
}


template <typename T>
Option<T> max(const T& left, const Option<T>& right)
{
  return max(Option<T>(left), right);
}

#endif // __STOUT_OPTION_HPP__
