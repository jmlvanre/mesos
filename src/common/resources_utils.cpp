// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <mesos/roles.hpp>

#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/stringify.hpp>
#include <stout/unreachable.hpp>

#include "common/resources_utils.hpp"

using std::ostream;
using std::set;
using std::string;

using google::protobuf::RepeatedPtrField;

namespace mesos {

namespace cpp {

THREAD_LOCAL Resource::Names* Resource::__names = new Resource::Names();

Resource::Names::GlobalNames* Resource::Names::__global_names = new Resource::Names::GlobalNames();

Resource::Resource(const ::mesos::Resource& resource)
  : name_(__names->getKey(resource.name())), value_(0.0)
{
  switch (resource.type()) {
    case ::mesos::Value::SCALAR: {
      value_ = Value(resource.scalar().value());
      break;
    }
    case ::mesos::Value::RANGES: {
      value_ = Value(resource.ranges());
      break;
    }
    case ::mesos::Value::SET: {
      value_ = Value(resource.set());
      break;
    }
    case ::mesos::Value::TEXT: {
      ABORT("This shouldn't be possible if the proto is validated before we "
            "convert inside the factory");
      break;
    }
  }

  if (resource.has_role()) {
    role_ = resource.role();
  }

  if (resource.has_reservation()) {
    reservation_ = ReservationInfo(resource.reservation());
  }

  if (resource.has_disk()) {
    disk_ = DiskInfo(resource.disk());
  }

  if (resource.has_revocable()) {
    revocable_ = RevocableInfo();
  }

  CHECK(Resources::validate(*this).isNone());
}


bool operator==(const Resource& left, const Resource& right)
{
  if (left.name_code() != right.name_code() ||
      left.type() != right.type() ||
      left.role() != right.role()) {
    return false;
  }

  // Check ReservationInfo.
  if (left.has_reservation() != right.has_reservation()) {
    return false;
  }

  if (left.has_reservation() && left.reservation() != right.reservation()) {
    return false;
  }

  // Check DiskInfo.
  if (left.has_disk() != right.has_disk()) {
    return false;
  }

  if (left.has_disk() && left.disk() != right.disk()) {
    return false;
  }

  // Check RevocableInfo.
  if (left.has_revocable() != right.has_revocable()) {
    return false;
  }

  return left.value() == right.value();
}


bool operator!=(const Resource& left, const Resource& right)
{
  return !(left == right);
}

namespace internal {

// Tests if we can add two Resource objects together resulting in one
// valid Resource object. For example, two Resource objects with
// different name, type or role are not addable.
static bool addable(const Resource& left, const Resource& right)
{
  if (left.name_code() != right.name_code() ||
      left.type() != right.type() ||
      left.role() != right.role()) {
    return false;
  }

  // Check ReservationInfo.
  if (left.has_reservation() != right.has_reservation()) {
    return false;
  }

  if (left.has_reservation() && left.reservation() != right.reservation()) {
    return false;
  }

  // Check DiskInfo.
  if (left.has_disk() != right.has_disk()) { return false; }

  if (left.has_disk()) {
    if (left.disk() != right.disk()) { return false; }

    // Two Resources that represent exclusive 'MOUNT' disks cannot be
    // added together; this would defeat the exclusivity.
    if (left.disk().has_source() &&
        left.disk().source().type() == Resource::DiskInfo::Source::MOUNT) {
      return false;
    }

    // TODO(jieyu): Even if two Resource objects with DiskInfo have
    // the same persistence ID, they cannot be added together. In
    // fact, this shouldn't happen if we do not add resources from
    // different namespaces (e.g., across slave). Consider adding a
    // warning.
    if (left.disk().has_persistence()) {
      return false;
    }
  }

  // Check RevocableInfo.
  if (left.has_revocable() != right.has_revocable()) {
    return false;
  }

  return true;
}


// Tests if we can subtract "right" from "left" resulting in one valid
// Resource object. For example, two Resource objects with different
// name, type or role are not subtractable.
// NOTE: Set subtraction is always well defined, it does not require
// 'right' to be contained within 'left'. For example, assuming that
// "left = {1, 2}" and "right = {2, 3}", "left" and "right" are
// subtractable because "left - right = {1}". However, "left" does not
// contain "right".
static bool subtractable(const Resource& left, const Resource& right)
{
  if (left.name_code() != right.name_code() ||
      left.type() != right.type() ||
      left.role() != right.role()) {
    return false;
  }

  // Check ReservationInfo.
  if (left.has_reservation() != right.has_reservation()) {
    return false;
  }

  if (left.has_reservation() && left.reservation() != right.reservation()) {
    return false;
  }

  // Check DiskInfo.
  if (left.has_disk() != right.has_disk()) { return false; }

  if (left.has_disk()) {
    if (left.disk() != right.disk()) { return false; }

    // Two Resources that represent exclusive 'MOUNT' disks cannot be
    // subtracted from eachother if they are not the exact same mount;
    // this would defeat the exclusivity.
    if (left.disk().has_source() &&
        left.disk().source().type() == Resource::DiskInfo::Source::MOUNT &&
        left != right) {
      return false;
    }

    // NOTE: For Resource objects that have DiskInfo, we can only do
    // subtraction if they are equal.
    if (left.disk().has_persistence() && left != right) {
      return false;
    }
  }

  // Check RevocableInfo.
  if (left.has_revocable() != right.has_revocable()) {
    return false;
  }

  return true;
}


// Tests if "right" is contained in "left".
static bool contains(const Resource& left, const Resource& right)
{
  // NOTE: This is a necessary condition for 'contains'.
  // 'subtractable' will verify name, role, type, ReservationInfo,
  // DiskInfo and RevocableInfo compatibility.
  if (!subtractable(left, right)) {
    return false;
  }

  return right.value() <= left.value();
}


static bool usable(const Resource& resource)
{
  return ((resource.type() != Resource::Value::SCALAR) || !(resource.scalar() < 0));
}

} // namespace internal {


template <>
Option<double> Resources::get(const string& name) const
{
  const uint64_t name_code = Resource::__names->getKey(name);
  double result = 0.0;
  bool found = false;

  foreach (const Resource& resource, resources) {
    if (resource.name_code() == name_code &&
        resource.type() == Resource::Value::SCALAR) {
      result += resource.scalar();
      found = true;
    }
  }

  if (found) {
    return result;
  }

  return None();
}


template <>
Option<IntervalSet<u_int64_t>> Resources::get(const string& name) const
{
  const uint64_t name_code = Resource::__names->getKey(name);
  IntervalSet<uint64_t> result;
  bool found = false;

  foreach (const Resource& resource, resources) {
    if (resource.name_code() == name_code &&
        resource.type() == Resource::Value::RANGES) {
      result += resource.ranges();
      found = true;
    }
  }

  if (found) {
    return result;
  }

  return None();
}


Resources Resources::get(const string& name) const
{
  const uint64_t name_code = Resource::__names->getKey(name);
  return filter([=](const Resource& resource) {
    return resource.name_code() == name_code;
  });
}


Resources Resources::scalars() const
{
  return filter([=](const Resource& resource) {
    return resource.type() == Resource::Value::SCALAR;
  });
}


set<string> Resources::names() const
{
  set<string> result;
  foreach (const Resource& resource, resources) {
    result.insert(resource.name());
  }

  return result;
}


Option<double> Resources::cpus() const
{
  Option<double> value = get<double>("cpus");
  if (value.isSome()) {
    return value.get();
  } else {
    return None();
  }
}


Option<Bytes> Resources::mem() const
{
  Option<double> value = get<double>("mem");
  if (value.isSome()) {
    return Megabytes(static_cast<uint64_t>(value.get()));
  } else {
    return None();
  }
}


Option<Bytes> Resources::disk() const
{
  Option<double> value = get<double>("disk");
  if (value.isSome()) {
    return Megabytes(static_cast<uint64_t>(value.get()));
  } else {
    return None();
  }
}


Option<IntervalSet<uint64_t>> Resources::ports() const
{
  Option<IntervalSet<uint64_t>> value = get<IntervalSet<uint64_t>>("ports");
  if (value.isSome()) {
    return value.get();
  } else {
    return None();
  }
}


Option<IntervalSet<uint64_t>> Resources::ephemeral_ports() const
{
  Option<IntervalSet<uint64_t>> value =
    get<IntervalSet<uint64_t>>("ephemeral_ports");

  if (value.isSome()) {
    return value.get();
  } else {
    return None();
  }
}


bool Resources::isEmpty(const Resource& resource)
{
  if (resource.type() == Resource::Value::SCALAR) {
    return resource.scalar() == 0;
  } else if (resource.type() == Resource::Value::RANGES) {
    return resource.ranges().size() == 0;
  } else if (resource.type() == Resource::Value::SET) {
    return resource.set().size() == 0;
  } else {
    return false;
  }
}


bool Resources::isPersistentVolume(const Resource& resource)
{
  return resource.has_disk() && resource.disk().has_persistence();
}


bool Resources::isReserved(
    const Resource& resource,
    const Option<string>& role)
{
  if (role.isSome()) {
    return !isUnreserved(resource) && role.get() == resource.role();
  } else {
    return !isUnreserved(resource);
  }
}


bool Resources::isUnreserved(const Resource& resource)
{
  return resource.role() == "*" && !resource.has_reservation();
}


bool Resources::isDynamicallyReserved(const Resource& resource)
{
  return resource.has_reservation();
}


bool Resources::isRevocable(const Resource& resource)
{
  return resource.has_revocable();
}


Resources::Resources(const Resource& resource)
{
  // NOTE: Invalid and zero Resource object will be ignored.
  *this += resource;
}


Option<Error> Resources::validate(const Resource& resource)
{
  if (resource.name().empty()) {
    return Error("Empty resource name");
  }

  if (!Value::Type_IsValid(resource.type())) {
    return Error("Invalid resource type");
  }

  if (resource.type() == Resource::Value::SCALAR) {
    if (resource.scalar() < 0) {
      return Error("Invalid scalar resource: value < 0");
    }
  } else if (resource.type() == Resource::Value::TEXT) {
    // Resource doesn't support TEXT or other value types.
    return Error("Unsupported resource type");
  }

  // Checks for 'disk' resource.
  if (resource.has_disk()) {
    if (resource.name() != "disk") {
      return Error(
          "DiskInfo should not be set for " + resource.name() + " resource");
    }
  }

  // Checks for the invalid state of (role, reservation) pair.
  if (resource.role() == "*" && resource.has_reservation()) {
    return Error(
        "Invalid reservation: role \"*\" cannot be dynamically reserved");
  }

  // Check role name.
  Option<Error> error = roles::validate(resource.role());
  if (error.isSome()) {
    return error;
  }

  return None();
}


Option<Error> Resources::validate(const RepeatedPtrField<Resource>& resources)
{
  foreach (const Resource& resource, resources) {
    Option<Error> error = validate(resource);
    if (error.isSome()) {
      return Error(
          "Resource '" + stringify(resource) +
          "' is invalid: " + error.get().message);
    }
  }

  return None();
}


Option<Error> Resources::validate(const Resources& resources)
{
  foreach (const Resource& resource, resources) {
    Option<Error> error = validate(resource);
    if (error.isSome()) {
      return Error(
          "Resource '" + stringify(resource) +
          "' is invalid: " + error.get().message);
    }
  }

  return None();
}


bool Resources::contains(const Resources& that) const
{
  Resources remaining = *this;

  foreach (const Resource& resource, that.resources) {
    // NOTE: We use _contains because Resources only contain valid
    // Resource objects, and we don't want the performance hit of the
    // validity check.
    if (!remaining._contains(resource)) {
      return false;
    }

    remaining -= resource;
  }

  return true;
}


bool Resources::contains(const Resource& that) const
{
  // NOTE: We must validate 'that' because invalid resources can lead
  // to false positives here (e.g., "cpus:-1" will return true). This
  // is because 'contains' assumes resources are valid.
  //return validate(that).isNone() && _contains(that);

  return internal::usable(that) && _contains(that);
}


Resources Resources::filter(
    const lambda::function<bool(const Resource&)>& predicate) const
{
  Resources result;
  foreach (const Resource& resource, resources) {
    if (predicate(resource)) {
      result += resource;
    }
  }
  return result;
}


Resources Resources::reserved(const string& role) const
{
  return filter(lambda::bind(isReserved, lambda::_1, role));
}


Resources Resources::unreserved() const
{
  return filter(isUnreserved);
}


Resources Resources::persistentVolumes() const
{
  return filter(isPersistentVolume);
}


Resources Resources::revocable() const
{
  return filter(isRevocable);
}


Resources Resources::nonRevocable() const
{
  return filter(
      [](const Resource& resource) { return !isRevocable(resource); });
}


Resources Resources::flatten(
    const string& role,
    const Option<Resource::ReservationInfo>& reservation) const
{
  Resources flattened;

  foreach (Resource resource, resources) {
    resource.set_role(role);
    if (reservation.isNone()) {
      resource.clear_reservation();
    } else {
      resource.mutable_reservation() = reservation.get();
    }
    flattened += resource;
  }

  return flattened;
}


Try<Resources> Resources::apply(const Offer::Operation& operation) const
{
  Resources result = *this;

  switch (operation.type()) {
    case Offer::Operation::LAUNCH:
      // Launch operation does not alter the offered resources.
      break;

    case Offer::Operation::RESERVE: {
      Option<Error> error =
        validate(cpp::Resources(operation.reserve().resources()));

      if (error.isSome()) {
        return Error("Invalid RESERVE Operation: " + error.get().message);
      }

      foreach (const cpp::Resource& reserved, operation.reserve().resources()) {
        if (!Resources::isReserved(reserved)) {
          return Error("Invalid RESERVE Operation: Resource must be reserved");
        } else if (!reserved.has_reservation()) {
          return Error("Invalid RESERVE Operation: Missing 'reservation'");
        }

        Resources unreserved = Resources(reserved).flatten();

        if (!result.contains(unreserved)) {
          return Error("Invalid RESERVE Operation: " + stringify(result) +
                       " does not contain " + stringify(unreserved));
        }

        result -= unreserved;
        result += reserved;
      }
      break;
    }

    case Offer::Operation::UNRESERVE: {
      Option<Error> error =
        validate(cpp::Resources(operation.unreserve().resources()));

      if (error.isSome()) {
        return Error("Invalid UNRESERVE Operation: " + error.get().message);
      }

      foreach (
          const cpp::Resource& reserved,
          operation.unreserve().resources()) {
        if (!Resources::isReserved(reserved)) {
          return Error("Invalid UNRESERVE Operation: Resource is not reserved");
        } else if (!reserved.has_reservation()) {
          return Error("Invalid UNRESERVE Operation: Missing 'reservation'");
        }

        if (!result.contains(reserved)) {
          return Error("Invalid UNRESERVE Operation: " + stringify(result) +
                       " does not contain " + stringify(reserved));
        }

        Resources unreserved = Resources(reserved).flatten();

        result -= reserved;
        result += unreserved;
      }
      break;
    }

    case Offer::Operation::CREATE: {
      Option<Error> error =
        validate(cpp::Resources(operation.create().volumes()));

      if (error.isSome()) {
        return Error("Invalid CREATE Operation: " + error.get().message);
      }

      foreach (const cpp::Resource& volume, operation.create().volumes()) {
        if (!volume.has_disk()) {
          return Error("Invalid CREATE Operation: Missing 'disk'");
        } else if (!volume.disk().has_persistence()) {
          return Error("Invalid CREATE Operation: Missing 'persistence'");
        }

        // Strip persistence and volume from the disk info so that we
        // can subtract it from the original resources.
        // TODO(jieyu): Non-persistent volumes are not supported for
        // now. Persistent volumes can only be be created from regular
        // disk resources. Revisit this once we start to support
        // non-persistent volumes.
        Resource stripped = volume;

        if (stripped.disk().has_source()) {
          stripped.mutable_disk().clear_persistence();
          stripped.mutable_disk().clear_volume();
        } else {
          stripped.clear_disk();
        }

        if (!result.contains(stripped)) {
          return Error("Invalid CREATE Operation: Insufficient disk resources");
        }

        result -= stripped;
        result += volume;
      }
      break;
    }

    case Offer::Operation::DESTROY: {
      Option<Error> error =
        validate(cpp::Resources(operation.destroy().volumes()));

      if (error.isSome()) {
        return Error("Invalid DESTROY Operation: " + error.get().message);
      }

      foreach (const cpp::Resource& volume, operation.destroy().volumes()) {
        if (!volume.has_disk()) {
          return Error("Invalid DESTROY Operation: Missing 'disk'");
        } else if (!volume.disk().has_persistence()) {
          return Error("Invalid DESTROY Operation: Missing 'persistence'");
        }

        if (!result.contains(volume)) {
          return Error(
              "Invalid DESTROY Operation: Persistent volume does not exist");
        }

        // Strip persistence and volume from the disk info so that we
        // can subtract it from the original resources.
        Resource stripped = volume;

        if (stripped.disk().has_source()) {
          stripped.mutable_disk().clear_persistence();
          stripped.mutable_disk().clear_volume();
        } else {
          stripped.clear_disk();
        }

        result -= volume;
        result += stripped;
      }
      break;
    }

    default:
      return Error("Unknown offer operation " + stringify(operation.type()));
  }

  // The following are sanity checks to ensure the amount of each type of
  // resource does not change.
  // TODO(jieyu): Currently, we only check known resource types like
  // cpus, mem, disk, ports, etc. We should generalize this.

  CHECK(result.cpus() == cpus());
  CHECK(result.mem() == mem());
  CHECK(result.disk() == disk());
  CHECK(result.ports() == ports());

  return result;
}


bool Resources::operator==(const Resources& that) const
{
  return this->contains(that) && that.contains(*this);
}


bool Resources::operator!=(const Resources& that) const
{
  return !(*this == that);
}


Resources Resources::operator+(const Resource& that) const
{
  Resources result = *this;
  result += that;
  return result;
}


Resources Resources::operator+(const Resources& that) const
{
  Resources result = *this;
  result += that;
  return result;
}


Resources& Resources::operator+=(const Resource& that)
{
  if (internal::usable(that) && !isEmpty(that)) {
    bool found = false;
    foreach (Resource& resource, resources) {
      if (internal::addable(resource, that)) {
        resource += that;
        found = true;
        break;
      }
    }

    // Cannot be combined with any existing Resource object.
    if (!found) {
      resources.emplace_back(that);
    }
  }

  return *this;
}


Resources& Resources::operator+=(const Resources& that)
{
  foreach (const Resource& resource, that.resources) {
    *this += resource;
  }

  return *this;
}


Resources Resources::operator-(const Resource& that) const
{
  Resources result = *this;
  result -= that;
  return result;
}


Resources Resources::operator-(const Resources& that) const
{
  Resources result = *this;
  result -= that;
  return result;
}


Resources& Resources::operator-=(const Resource& that)
{
  if (internal::usable(that) && !isEmpty(that)) {
    for (int i = 0; i < resources.size(); i++) {
      Resource& resource = resources[i];

      if (internal::subtractable(resource, that)) {
        resource -= that;

        // Remove the resource if it becomes invalid or zero. We need
        // to do the validation because we want to strip negative
        // scalar Resource object.
        if (!internal::usable(resource) || isEmpty(resource)) {
          // As `resources` is not ordered, and erasing an element
          // from the middle using `DeleteSubrange` is expensive, we
          // swap with the last element and then shrink the
          // 'RepeatedPtrField' by one.
          std::swap(resources[i], resources[resources.size() - 1]);
          resources.pop_back();
        }

        break;
      }
    }
  }

  return *this;
}


Resources& Resources::operator-=(const Resources& that)
{
  foreach (const Resource& resource, that.resources) {
    *this -= resource;
  }

  return *this;
}


::mesos::Resources Resources::proto() const
{
  RepeatedPtrField<::mesos::Resource> result;
  foreach (const Resource& _resource, resources) {
    ::mesos::Resource* resource = result.Add();
    resource->set_name(_resource.name());
    switch (_resource.type()) {
      case Resource::Value::SCALAR: {
        resource->set_type(::mesos::Value::SCALAR);
        ::mesos::Value::Scalar* scalar = resource->mutable_scalar();
        scalar->set_value(_resource.scalar());
        break;
      }
      case Resource::Value::RANGES: {
        resource->set_type(::mesos::Value::RANGES);
        ::mesos::Value::Ranges* ranges = resource->mutable_ranges();
        foreach (const Interval<uint64_t>& interval, _resource.ranges()) {
          ::mesos::Value::Range* range = ranges->add_range();
          range->set_begin(interval.lower());
          range->set_end(interval.upper() - 1);
        }
        break;
      }
      case Resource::Value::SET: {
        resource->set_type(::mesos::Value::SET);
        ::mesos::Value::Set* set = resource->mutable_set();
        foreach (const std::string& value, _resource.set()) {
          set->add_item(value);
        }
        break;
      }
      case Resource::Value::TEXT: {
        UNREACHABLE();
        break;
      }

      if (_resource.has_role()) {
        resource->set_role(_resource.role());
      }

      if (_resource.has_reservation()) {
        ::mesos::Resource::ReservationInfo* reservation =
          resource->mutable_reservation();

        if (_resource.reservation().has_principal()) {
          reservation->set_principal(_resource.reservation().principal());
        }

        if (_resource.reservation().has_labels()) {
          ::mesos::Labels* labels = reservation->mutable_labels();
          for (
              const std::pair<std::string, std::string>& _label :
              _resource.reservation().labels().labels) {
            ::mesos::Label* label = labels->add_labels();
            label->set_key(_label.first);
            label->set_value(_label.second);
          }
        }
      }

      if (_resource.has_disk()) {
        ::mesos::Resource::DiskInfo* disk = resource->mutable_disk();
        if (_resource.disk().has_persistence()) {
          ::mesos::Resource::DiskInfo::Persistence* persistence =
            disk->mutable_persistence();

          persistence->set_principal(_resource.disk().persistence().id());

          if (_resource.disk().persistence().has_principal()) {
            persistence->set_principal(
                _resource.disk().persistence().principal());
          }
        }

        if (_resource.disk().has_volume()) {
          ::mesos::Volume* volume = disk->mutable_volume();
          switch (_resource.disk().volume().mode()) {
            case Volume::RW: {
              volume->set_mode(::mesos::Volume::RW);
              break;
            }
            case Volume::RO: {
              volume->set_mode(::mesos::Volume::RO);
              break;
            }
          }

          volume->set_container_path(
              _resource.disk().volume().container_path());

          if (_resource.disk().volume().has_host_path()) {
            volume->set_host_path(_resource.disk().volume().host_path());
          }

          if (_resource.disk().volume().has_image()) {
            ::mesos::Image* image = volume->mutable_image();
            image->CopyFrom(_resource.disk().volume().image());
          }
        }

        if (_resource.disk().has_source()) {
          ::mesos::Resource::DiskInfo::Source* source = disk->mutable_source();
          switch (_resource.disk().source().type()) {
            case Resource::DiskInfo::Source::PATH: {
              source->set_type(::mesos::Resource::DiskInfo::Source::PATH);
              ::mesos::Resource::DiskInfo::Source::Path* path =
                source->mutable_path();

              path->set_root(_resource.disk().source().path());
              break;
            }
            case Resource::DiskInfo::Source::MOUNT: {
              source->set_type(::mesos::Resource::DiskInfo::Source::MOUNT);
              ::mesos::Resource::DiskInfo::Source::Mount* mount =
                source->mutable_mount();

              mount->set_root(_resource.disk().source().mount());
              break;
            }
          }
        }
      }

      if (_resource.has_revocable()) {
        resource->mutable_revocable();
      }
    }
  }

  return result;
}


bool Resources::_contains(const Resource& that) const
{
  foreach (const Resource& resource, resources) {
    if (internal::contains(resource, that)) {
      return true;
    }
  }

  return false;
}


ostream& operator<<(ostream& stream, const Volume& volume)
{
  string volumeConfig = volume.container_path();

  if (volume.has_host_path()) {
    volumeConfig = volume.host_path() + ":" + volumeConfig;

    switch (volume.mode()) {
      case Volume::RW: volumeConfig += ":rw"; break;
      case Volume::RO: volumeConfig += ":ro"; break;
      default:
        LOG(FATAL) << "Unknown Volume mode: " << volume.mode();
        break;
    }
  }

  stream << volumeConfig;

  return stream;
}


ostream& operator<<(ostream& stream, const Resource::DiskInfo& disk)
{
  if (disk.has_persistence()) {
    stream << disk.persistence().id();
  }

  if (disk.has_volume()) {
    stream << ":" << disk.volume();
  }

  return stream;
}


ostream& operator<<(
    ostream& stream,
    const Resource::ReservationInfo::Labels& labels)
{
  stream << "{";

  for (
      auto iterator = labels.labels.begin();
      iterator != labels.labels.end();
      ++iterator) {
    const std::pair<const std::string, std::string>& label = *iterator;

    stream << label.first << ": " << label.second;

    if (iterator != (--labels.labels.end())) {
      stream << ", ";
    }
  }

  stream << "}";

  return stream;
}


ostream& operator<<(ostream& stream, const Resource& resource)
{
  stream << resource.name();

  stream << "(" << resource.role();

  if (resource.has_reservation()) {
    const Resource::ReservationInfo& reservation = resource.reservation();

    if (reservation.has_principal()) {
      stream << ", " << reservation.principal();
    }

    if (reservation.has_labels()) {
      stream << ", " << reservation.labels();
    }
  }

  stream << ")";

  if (resource.has_disk()) {
    stream << "[" << resource.disk() << "]";
  }

  // Once extended revocable attributes are available, change this to a more
  // meaningful value.
  if (resource.has_revocable()) {
    stream << "{REV}";
  }

  stream << ":";

  switch (resource.type()) {
    case Value::SCALAR: stream << resource.scalar(); break;
    case Value::RANGES: stream << resource.ranges(); break;
    case Value::SET:    ABORT("Implement print set");break;
    default:
      LOG(FATAL) << "Unexpected Value type: " << resource.type();
      break;
  }

  return stream;
}


ostream& operator<<(ostream& stream, const Resources& resources)
{
  Resources::const_iterator it = resources.begin();

  while (it != resources.end()) {
    stream << *it;
    if (++it != resources.end()) {
      stream << "; ";
    }
  }

  return stream;
}

} // namespace cpp

bool needCheckpointing(const Resource& resource)
{
  return Resources::isDynamicallyReserved(resource) ||
         Resources::isPersistentVolume(resource);
}


// NOTE: We effectively duplicate the logic in 'Resources::apply'
// which is less than ideal. But we can not simply create
// 'Offer::Operation' and invoke 'Resources::apply' here.
// 'RESERVE' operation requires that the specified resources are
// dynamically reserved only, and 'CREATE' requires that the
// specified resources are already dynamically reserved.
// These requirements are violated when we try to infer dynamically
// reserved persistent volumes.
// TODO(mpark): Consider introducing an atomic 'RESERVE_AND_CREATE'
// operation to solve this problem.
Try<Resources> applyCheckpointedResources(
    const Resources& resources,
    const Resources& checkpointedResources)
{
  Resources totalResources = resources;

  foreach (const Resource& resource, checkpointedResources) {
    if (!needCheckpointing(resource)) {
      return Error("Unexpected checkpointed resources " + stringify(resource));
    }

    Resource stripped = resource;

    if (Resources::isDynamicallyReserved(resource)) {
      stripped.set_role("*");
      stripped.clear_reservation();
    }

    // Strip persistence and volume from the disk info so that we can
    // check whether it is contained in the `totalResources`.
    if (Resources::isPersistentVolume(resource)) {
      if (stripped.disk().has_source()) {
        stripped.mutable_disk()->clear_persistence();
        stripped.mutable_disk()->clear_volume();
      } else {
        stripped.clear_disk();
      }
    }

    if (!totalResources.contains(stripped)) {
      return Error(
          "Incompatible slave resources: " + stringify(totalResources) +
          " does not contain " + stringify(stripped));
    }

    totalResources -= stripped;
    totalResources += resource;
  }

  return totalResources;
}

} // namespace mesos {
