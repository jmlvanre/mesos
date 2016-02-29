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

#ifndef __RESOURCES_UTILS_HPP__
#define __RESOURCES_UTILS_HPP__

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>

#include <stout/interval.hpp>
#include <stout/try.hpp>

namespace mesos {

namespace cpp {

class Volume
{
public:
  enum Mode
  {
    RW,
    RO
  };

  Volume(const ::mesos::Volume& volume)
    : container_path_(volume.container_path())
  {
    switch (volume.mode()) {
      case ::mesos::Volume::RW: {
        mode_ = RW;
        break;
      }
      case ::mesos::Volume::RO: {
        mode_ = RO;
        break;
      }
    }

    if (volume.has_host_path()) {
      host_path_ = volume.host_path();
    }

    if (volume.has_image()) {
      image_ = volume.image();
    }
  }

  const Mode& mode() const { return mode_; }

  const std::string& container_path() const { return container_path_; }

  bool has_host_path() const { return host_path_.isSome(); }

  const std::string& host_path() const { return host_path_.get(); }

  bool has_image() const { return image_.isSome(); }

  const Image& image() const { return image_.get(); }

private:
  Mode mode_;
  std::string container_path_;
  Option<std::string> host_path_;
  Option<Image> image_;
};

class Resource
{
public:
  class Value
  {
  public:
    enum Type
    {
      SCALAR,
      RANGES,
      SET,
      TEXT
    };

    Value(double scalar) : type_(SCALAR), scalar_(scalar) {}

    Value(const ::mesos::Value::Ranges& ranges) : type_(RANGES), ranges_()
    {
      foreach (const ::mesos::Value::Range& range, ranges.range()) {
        const Interval<uint64_t> interval =
          (Bound<uint64_t>::closed(range.begin()),
           Bound<uint64_t>::closed(range.end()));

        ranges_ += interval;
      }
    }

    Value(const ::mesos::Value::Set& set)
      : type_(SET),
        set_(set.item().begin(), set.item().end()) {}

    Value(const ::mesos::Value::Text& text) : type_(TEXT), text_(text.value()) {}

    Value(const Value& that) : Value(0.0)
    {
      *this = that;
    }

    ~Value()
    {
      switch (type_) {
        case SCALAR: {
          // Do nothing.
          break;
        }
        case RANGES: {
          ranges_.~IntervalSet<uint64_t>();
          break;
        }
        case SET: {
          set_.~unordered_set<std::string>();
          break;
        }
        case TEXT: {
          text_.~basic_string<char>();
          break;
        }
      }
    }

    Value& operator=(const Value& that)
    {
      if (this != &that) {
        this->~Value();

        type_ = that.type_;

        switch (that.type_) {
          case SCALAR: {
            scalar_ = that.scalar_;
            break;
          }
          case RANGES: {
            new (&ranges_) IntervalSet<uint64_t>(that.ranges_);
            break;
          }
          case SET: {
            new (&set_) std::unordered_set<std::string>(that.set_);
            break;
          }
          case TEXT: {
            new (&text_) std::string(that.text_);
            break;
          }
        }
      }

      return *this;
    }

    bool operator==(const Value& that) const
    {
      if (type() != that.type()) {
        return false;
      }

      switch (that.type_) {
        case SCALAR: {
          return scalar() == that.scalar();
        }
        case RANGES: {
          return ranges() == that.ranges();
        }
        case SET: {
          return set() == that.set();
        }
        case TEXT: {
          return text() == that.text();
        }
      }
    }

    bool operator<(const Value& that) const
    {
      if (type() != that.type() || type() == TEXT) {
        return false;
      }

      switch (that.type_) {
        case SCALAR: {
          return scalar() < that.scalar();
        }
        case RANGES: {
          ABORT("TODO: implement ranges operator<");
          //return ranges() < that.ranges();
        }
        case SET: {
          ABORT("TODO: implement set operator<");
        }
        case TEXT: {
          break;
        }
      }

      return false;
    }

    bool operator<=(const Value& that) const
    {
      return (*this < that) || (*this == that);
    }

    Value& operator+=(const Value& that)
    {
      // TODO(jmlvanre): We really shouldn't allow this silent failure.
      // Consider throwing an error.
      if (type() != that.type() || type() == TEXT) { return *this; }

      switch (that.type_) {
        case SCALAR: {
          scalar_ += that.scalar();
          break;
        }
        case RANGES: {
          ranges_ += that.ranges();
          break;
        }
        case SET: {
          set_.insert(that.set().begin(), that.set().end());
          break;
        }
        case TEXT: {
          // Silent failure.
          break;
        }
      }

      return *this;
    }

    Value& operator-=(const Value& that)
    {
      // TODO(jmlvanre): We really shouldn't allow this silent failure.
      // Consider throwing an error.
      if (type() != that.type() || type() == TEXT) { return *this; }

      switch (that.type_) {
        case SCALAR: {
          scalar_ -= that.scalar();
          break;
        }
        case RANGES: {
          ranges_ -= that.ranges();
          break;
        }
        case SET: {
          foreach (const std::string& key, that.set()) {
            set_.erase(key);
          }
          break;
        }
        case TEXT: {
          // Silent failure.
          break;
        }
      }

      return *this;
    }

    Value& operator+=(double value)
    {
      // TODO(jmlvanre): We really shouldn't allow this silent failure.
      // Consider throwing an error.
      if (type() == SCALAR) {
        scalar_ += value;
      }

      return *this;
    }

    Value& operator-=(double value)
    {
      // TODO(jmlvanre): We really shouldn't allow this silent failure.
      // Consider throwing an error.
      if (type() == SCALAR) {
        scalar_ -= value;
      }

      return *this;
    }

    const Type& type() const { return type_; }

    double scalar() const { return scalar_; }

    const IntervalSet<uint64_t>& ranges() const { return ranges_; }

    const std::unordered_set<std::string>& set() const { return set_; }

    const std::string& text() const { return text_; }

  private:

    Type type_;

    union {
      double scalar_;
      IntervalSet<uint64_t> ranges_;
      std::unordered_set<std::string> set_;
      std::string text_;
    };
  };

  struct ReservationInfo
  {
    Option<std::string> principal_;

    struct Labels
    {
      std::map<std::string, std::string> labels;
    };

    Option<Labels> labels_;

    ReservationInfo(const ::mesos::Resource::ReservationInfo& reservation)
    {
      if (reservation.has_principal()) {
        principal_ = reservation.principal();
      }

      if (reservation.has_labels()) {
        labels_ = Labels();
        foreach (const ::mesos::Label& label, reservation.labels().labels()) {
          labels_.get().labels[label.key()] = label.value();
        }
      }
    }

    bool has_principal() const { return principal_.isSome(); }

    const std::string& principal() const { return principal_.get(); }

    bool has_labels() const { return labels_.isSome(); }

    const Labels labels() const
    {
      return labels_.get();
    }

    bool operator==(const ReservationInfo& that) const
    {
      if (has_principal() != that.has_principal()) {
        return false;
      }

      if (has_principal() && principal() != that.principal()) {
        return false;
      }

      if (has_labels() != that.has_labels()) {
        return false;
      }

      if (has_labels() && labels().labels != that.labels().labels) {
        return false;
      }

      return true;
    }

    bool operator!=(const ReservationInfo& that) const
    {
      return !(*this == that);
    }
  };

  class DiskInfo
  {
  public:
    class Persistence
    {
    public:
      Persistence(const ::mesos::Resource::DiskInfo::Persistence& persistence)
        : id_(persistence.id())
      {
        if (persistence.has_principal()) {
          principal_ = persistence.principal();
        }
      }

      const std::string& id() const { return id_; }

      bool has_principal() const { return principal_.isSome(); }

      const std::string& principal() const { return principal_.get(); }

    private:
      std::string id_;

      Option<std::string> principal_;
    };

    class Source
    {
    public:
      enum Type
      {
        PATH,
        MOUNT
      };

      Source(const ::mesos::Resource::DiskInfo::Source& source)
        : type_(source.type() == ::mesos::Resource::DiskInfo::Source::PATH ?
                 PATH : MOUNT) {
        switch (type_) {
          case PATH: {
            root_ = source.path().root();
            break;
          }
          case MOUNT: {
            root_ = source.mount().root();
            break;
          }
        }
      }

      const Type& type() const { return type_; }

      bool has_path() const { return type_ == PATH; }

      bool has_mount() const { return type_ == MOUNT; }

      const std::string& path() const { return root_; }

      const std::string& mount() const { return root_; }

      bool operator==(const Source& that) const
      {
        if (type() != that.type()) {
          return false;
        }

        if (has_path() && path() != that.path()) {
          return false;
        }

        if (has_mount() && mount() != that.mount()) {
          return false;
        }

        return true;
      }

      bool operator!=(const Source& that) const { return !(*this == that); }

    private:
      Type type_;
      std::string root_;
    };

    DiskInfo(const ::mesos::Resource::DiskInfo& disk)
    {
      if (disk.has_persistence()) {
        persistence_ = Persistence(disk.persistence());
      }

      if (disk.has_volume()) {
        volume_ = Volume(disk.volume());
      }

      if (disk.has_source()) {
        source_ = Source(disk.source());
      }
    }

    bool has_source() const { return source_.isSome(); }

    const Source& source() const { return source_.get(); }

    bool has_persistence() const { return persistence_.isSome(); }

    const Persistence& persistence() const { return persistence_.get(); }

    void clear_persistence() { persistence_ = None(); }

    bool has_volume() const { return volume_.isSome(); }

    const Volume& volume() const { return volume_.get(); }

    void clear_volume() { volume_ = None(); }

    bool operator==(const DiskInfo& that) const
    {
      if (has_source() != that.has_source()) {
        return false;
      }

      if (has_source() && source() != that.source()) {
        return false;
      }

      // NOTE: We ignore 'volume' inside DiskInfo when doing comparison
      // because it describes how this resource will be used which has
      // nothing to do with the Resource object itself. A framework can
      // use this resource and specify different 'volume' every time it
      // uses it.
      if (has_persistence() != that.has_persistence()) {
        return false;
      }

      if (has_persistence()) {
        return persistence().id() == that.persistence().id();
      }

      return true;
    }

    bool operator!=(const DiskInfo& that) const { return !(*this == that); }

  private:
    Option<Persistence> persistence_;

    Option<Volume> volume_;

    Option<Source> source_;
  };

  class RevocableInfo {};

  Resource(const ::mesos::Resource& resource)
    : name_(resource.name()), value_(0.0)
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
  }

  Resource(const Resource& that)
    : name_(that.name_),
      value_(that.value_),
      role_(that.role_),
      reservation_(that.reservation_),
      disk_(that.disk_),
      revocable_(that.revocable_) {}

  bool has_role() const { return role_.isSome(); }

  const std::string& role() const
  {
    static std::string defaultRole = "*";
    return has_role() ? role_.get() : defaultRole;
  }

  void set_role(const std::string& role) { role_ = role; }

  const Value::Type& type() const { return value_.type(); }

  const Value& value() const { return value_; }

  double scalar() const { return value_.scalar(); }

  const IntervalSet<uint64_t>& ranges() const { return value_.ranges(); }

  const std::unordered_set<std::string>& set() const { return value_.set(); }

  const std::string& text() const { return value_.text(); }

  const std::string& name() const { return name_; }

  bool has_reservation() const { return reservation_.isSome(); }

  const ReservationInfo& reservation() const { return reservation_.get(); }

  ReservationInfo& mutable_reservation() { return reservation_.get(); }

  void clear_reservation() { reservation_ = None(); }

  bool has_disk() const { return disk_.isSome(); }

  const DiskInfo& disk() const { return disk_.get(); }

  DiskInfo& mutable_disk() { return disk_.get(); }

  void clear_disk() { disk_ = None(); }

  bool has_revocable() const { return revocable_.isSome(); }

  void make_revocable() { revocable_ = RevocableInfo(); }

  Resource& operator+=(const Resource& that)
  {
    value_ += that.value();

    return *this;
  }

  Resource& operator-=(const Resource& that)
  {
    value_ -= that.value();

    return *this;
  }

private:

  std::string name_;

  Value value_;

  Option<std::string> role_;

  Option<ReservationInfo> reservation_;

  Option<DiskInfo> disk_;

  Option<RevocableInfo> revocable_;
};

class Resources
{
public:
  /**
   * Returns a Resource with the given name, value, and role.
   *
   * Parses the text and returns a Resource object with the given name, value,
   * and role. For example, "Resource r = parse("mem", "1024", "*");".
   *
   * @param name The name of the Resource.
   * @param value The Resource's value.
   * @param role The role associated with the Resource.
   * @return A `Try` which contains the parsed Resource if parsing was
   *     successful, or an Error otherwise.
   */
  static Try<Resource> parse(
      const std::string& name,
      const std::string& value,
      const std::string& role);

  /**
   * Parses Resources from an input string.
   *
   * Parses Resources from text in the form of a JSON array. If that fails,
   * parses text in the form "name(role):value;name:value;...". Any resource
   * that doesn't specify a role is assigned to the provided default role. See
   * the `Resource` protobuf definition for precise JSON formatting.
   *
   * Example JSON: [{"name":cpus","type":"SCALAR","scalar":{"value":8}}]
   *
   * @param text The input string.
   * @param defaultRole The default role.
   * @return A `Try` which contains the parsed Resources if parsing was
   *     successful, or an Error otherwise.
   */
  static Try<Resources> parse(
      const std::string& text,
      const std::string& defaultRole = "*");

  /**
   * Validates a Resource object.
   *
   * Validates the given Resource object. Returns Error if it is not valid. A
   * Resource object is valid if it has a name, a valid type, i.e. scalar,
   * range, or set, has the appropriate value set, and a valid (role,
   * reservation) pair for dynamic reservation.
   *
   * @param resource The input resource to be validated.
   * @return An `Option` which contains None() if the validation was successful,
   *     or an Error if not.
   */
  static Option<Error> validate(const Resource& resource);

  /**
   * Validates the given repeated Resource protobufs.
   *
   * Validates the given repeated Resource protobufs. Returns Error if an
   * invalid Resource is found. A Resource object is valid if it has a name, a
   * valid type, i.e. scalar, range, or set, has the appropriate value set, and
   * a valid (role, reservation) pair for dynamic reservation.
   *
   * TODO(jieyu): Right now, it's the same as checking each individual Resource
   * object in the protobufs. In the future, we could add more checks that are
   * not possible if checking each Resource object individually. For example, we
   * could check multiple usage of an item in a set or a range, etc.
   *
   * @param resources The repeated Resource objects to be validated.
   * @return An `Option` which contains None() if the validation was successful,
   *     or an Error if not.
   */
  static Option<Error> validate(
      const google::protobuf::RepeatedPtrField<Resource>& resources);

  static Option<Error> validate(const Resources& resources);

  // NOTE: The following predicate functions assume that the given
  // resource is validated.
  //
  // Valid states of (role, reservation) pair in the Resource object.
  //   Unreserved         : ("*", None)
  //   Static reservation : (R, None)
  //   Dynamic reservation: (R, { principal: <framework_principal> })
  //
  // NOTE: ("*", { principal: <framework_principal> }) is invalid.

  // Tests if the given Resource object is empty.
  static bool isEmpty(const Resource& resource);

  // Tests if the given Resource object is a persistent volume.
  static bool isPersistentVolume(const Resource& resource);

  // Tests if the given Resource object is reserved. If the role is
  // specified, tests that it's reserved for the given role.
  static bool isReserved(
      const Resource& resource,
      const Option<std::string>& role = None());

  // Tests if the given Resource object is unreserved.
  static bool isUnreserved(const Resource& resource);

  // Tests if the given Resource object is dynamically reserved.
  static bool isDynamicallyReserved(const Resource& resource);

  // Tests if the given Resource object is revocable.
  static bool isRevocable(const Resource& resource);

  // Returns the summed up Resources given a hashmap<Key, Resources>.
  //
  // NOTE: While scalar resources such as "cpus" sum correctly,
  // non-scalar resources such as "ports" do not.
  //   e.g. "cpus:2" + "cpus:1" = "cpus:3"
  //        "ports:[0-100]" + "ports:[0-100]" = "ports:[0-100]"
  //
  // TODO(mpark): Deprecate this function once we introduce the
  // concept of "cluster-wide" resources which provides correct
  // semantics for summation over all types of resources. (e.g.
  // non-scalar)
  template <typename Key>
  static Resources sum(const hashmap<Key, Resources>& _resources)
  {
    Resources result;

    foreachvalue (const Resources& resources, _resources) {
      result += resources;
    }

    return result;
  }

  Resources() {}

  // TODO(jieyu): Consider using C++11 initializer list.
  /*implicit*/ Resources(const Resource& resource);

  /*implicit*/
  Resources(const std::vector<Resource>& _resources);

  /*implicit*/
  Resources(const google::protobuf::RepeatedPtrField<Resource>& _resources);

  Resources(const Resources& that) : resources(that.resources) {}

  Resources(const ::mesos::Resources& that) : resources(that.begin(), that.end()) {}

  Resources& operator=(const Resources& that)
  {
    if (this != &that) {
      resources = that.resources;
    }
    return *this;
  }

  bool empty() const { return resources.size() == 0; }

  size_t size() const { return resources.size(); }

  // Checks if this Resources is a superset of the given Resources.
  bool contains(const Resources& that) const;

  // Checks if this Resources contains the given Resource.
  bool contains(const Resource& that) const;

  // Filter resources based on the given predicate.
  Resources filter(
      const lambda::function<bool(const Resource&)>& predicate) const;

  // Returns the reserved resources, by role.
  hashmap<std::string, Resources> reserved() const;

  // Returns the reserved resources for the role. Note that the "*"
  // role represents unreserved resources, and will be ignored.
  Resources reserved(const std::string& role) const;

  // Returns the unreserved resources.
  Resources unreserved() const;

  // Returns the persistent volumes.
  Resources persistentVolumes() const;

  // Returns the revocable resources.
  Resources revocable() const;

  // Returns the non-revocable resources, effectively !revocable().
  Resources nonRevocable() const;

  // Returns a Resources object with the same amount of each resource
  // type as these Resources, but with all Resource objects marked as
  // the specified (role, reservation) pair. This is used to cross
  // reservation boundaries without affecting the actual resources.
  // If the optional ReservationInfo is given, the resource's
  // 'reservation' field is set. Otherwise, the resource's
  // 'reservation' field is cleared.
  Resources flatten(
      const std::string& role = "*",
      const Option<Resource::ReservationInfo>& reservation = None()) const;

  // Finds a Resources object with the same amount of each resource
  // type as "targets" from these Resources. The roles specified in
  // "targets" set the preference order. For each resource type,
  // resources are first taken from the specified role, then from '*',
  // then from any other role.
  // TODO(jieyu): 'find' contains some allocation logic for scalars and
  // fixed set / range elements. However, this is not sufficient for
  // schedulers that want, say, any N available ports. We should
  // consider moving this to an internal "allocation" library for our
  // example frameworks to leverage.
  Option<Resources> find(const Resources& targets) const;

  // Certain offer operations (e.g., RESERVE, UNRESERVE, CREATE or
  // DESTROY) alter the offered resources. The following methods
  // provide a convenient way to get the transformed resources by
  // applying the given offer operation(s). Returns an Error if the
  // offer operation(s) cannot be applied.
  Try<Resources> apply(const Offer::Operation& operation) const;

  template <typename Iterable>
  Try<Resources> apply(const Iterable& operations) const
  {
    Resources result = *this;

    foreach (const Offer::Operation& operation, operations) {
      Try<Resources> transformed = result.apply(operation);
      if (transformed.isError()) {
        return Error(transformed.error());
      }

      result = transformed.get();
    }

    return result;
  }

  // Helpers to get resource values. We consider all roles here.
  template <typename T>
  Option<T> get(const std::string& name) const;

  // Get resources of the given name.
  Resources get(const std::string& name) const;

  // Get all the resources that are scalars.
  Resources scalars() const;

  // Get the set of unique resource names.
  std::set<std::string> names() const;

  // Get the types of resources associated with each resource name.
  // NOTE: Resources of the same name must have the same type, as
  // enforced by Resources::parse().
  std::map<std::string, Value_Type> types() const;

  // Helpers to get known resource types.
  // TODO(vinod): Fix this when we make these types as first class
  // protobufs.
  Option<double> cpus() const;
  Option<Bytes> mem() const;
  Option<Bytes> disk() const;

  // TODO(vinod): Provide a Ranges abstraction.
  Option<IntervalSet<uint64_t>> ports() const;

  // TODO(jieyu): Consider returning an EphemeralPorts abstraction
  // which holds the ephemeral ports allocation logic.
  Option<IntervalSet<uint64_t>> ephemeral_ports() const;

  // NOTE: Non-`const` `iterator`, `begin()` and `end()` are __intentionally__
  // defined with `const` semantics in order to prevent mutable access to the
  // `Resource` objects within `resources`.
  typedef std::vector<Resource>::const_iterator
  iterator;

  typedef std::vector<Resource>::const_iterator
  const_iterator;

  const_iterator begin()
  {
    return static_cast<const std::vector<Resource>&>(resources).begin();
  }

  const_iterator end()
  {
    return static_cast<const std::vector<Resource>&>(resources).end();
  }

  const_iterator begin() const { return resources.begin(); }
  const_iterator end() const { return resources.end(); }

  // Using this operator makes it easy to copy a resources object into
  // a protocol buffer field.
  //operator const google::protobuf::RepeatedPtrField<Resource>&() const;

  bool operator==(const Resources& that) const;
  bool operator!=(const Resources& that) const;

  // NOTE: If any error occurs (e.g., input Resource is not valid or
  // the first operand is not a superset of the second oprand while
  // doing subtraction), the semantics is as though the second operand
  // was actually just an empty resource (as though you didn't do the
  // operation at all).
  Resources operator+(const Resource& that) const;
  Resources operator+(const Resources& that) const;
  Resources& operator+=(const Resource& that);
  Resources& operator+=(const Resources& that);

  Resources operator-(const Resource& that) const;
  Resources operator-(const Resources& that) const;
  Resources& operator-=(const Resource& that);
  Resources& operator-=(const Resources& that);

  ::mesos::Resources proto() const;

private:
  // Similar to 'contains(const Resource&)' but skips the validity
  // check. This can be used to avoid the performance overhead of
  // calling 'contains(const Resource&)' when the resource can be
  // assumed valid (e.g. it's inside a Resources).
  //
  // TODO(jieyu): Measure performance overhead of validity check to
  // ensure this is warranted.
  bool _contains(const Resource& that) const;

  // Similar to the public 'find', but only for a single Resource
  // object. The target resource may span multiple roles, so this
  // returns Resources.
  Option<Resources> find(const Resource& target) const;

  std::vector<Resource> resources;
};


template <>
Option<::mesos::Value::Scalar> Resources::get(const std::string& name) const = delete;


std::ostream& operator<<(std::ostream& stream, const Resource& resource);


std::ostream& operator<<(std::ostream& stream, const Resources& resources);

} // namespace cpp {

// Tests if the given Resource needs to be checkpointed on the slave.
// NOTE: We assume the given resource is validated.
bool needCheckpointing(const Resource& resource);

// Returns the total resources by applying the given checkpointed
// resources to the given resources. This function is useful when we
// want to calculate the total resources of a slave from the resources
// specified from the command line and the checkpointed resources.
// Returns error if the given resources are not compatible with the
// given checkpointed resources.
Try<Resources> applyCheckpointedResources(
    const Resources& resources,
    const Resources& checkpointedResources);

} // namespace mesos {

#endif // __RESOURCES_UTILS_HPP__
