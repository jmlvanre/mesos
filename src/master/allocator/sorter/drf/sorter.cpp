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

#include "logging/logging.hpp"

#include "master/allocator/sorter/drf/sorter.hpp"

using std::list;
using std::set;
using std::string;

namespace mesos {
namespace internal {
namespace master {
namespace allocator {

bool DRFComparator::operator()(const Client& client1, const Client& client2)
{
  if (client1.share == client2.share) {
    if (client1.allocations == client2.allocations) {
      return client1.name < client2.name;
    }
    return client1.allocations < client2.allocations;
  }
  return client1.share < client2.share;
}


void DRFSorter::add(const string& name, double weight)
{
  Client client(name, 0, 0);
  clients.insert(client);

  allocations[name] = Allocation();
  weights[name] = weight;
}


void DRFSorter::update(const string& name, double weight)
{
  CHECK(weights.contains(name));
  weights[name] = weight;
}


void DRFSorter::remove(const string& name)
{
  set<Client, DRFComparator>::iterator it = find(name);

  if (it != clients.end()) {
    clients.erase(it);
  }

  allocations.erase(name);
  weights.erase(name);
}


void DRFSorter::activate(const string& name)
{
  CHECK(allocations.contains(name));

  Client client(name, calculateShare(name), 0);
  clients.insert(client);
}


void DRFSorter::deactivate(const string& name)
{
  set<Client, DRFComparator>::iterator it = find(name);

  if (it != clients.end()) {
    // TODO(benh): Removing the client is an unfortuante strategy
    // because we lose information such as the number of allocations
    // for this client which means the fairness can be gamed by a
    // framework disconnecting and reconnecting.
    clients.erase(it);
  }
}


void DRFSorter::allocated(
    const string& name,
    const SlaveID& slaveId,
    const cpp::Resources& resources)
{
  set<Client, DRFComparator>::iterator it = find(name);

  if (it != clients.end()) { // TODO(benh): This should really be a CHECK.
    // TODO(benh): Refactor 'update' to be able to reuse it here.
    Client client(*it);

    // Update the 'allocations' to reflect the allocator decision.
    client.allocations++;

    // Remove and reinsert it to update the ordering appropriately.
    clients.erase(it);
    clients.insert(client);
  }

  allocations[name].resources[slaveId] += resources;
  allocations[name].scalars += resources.scalars();

  // If the total resources have changed, we're going to
  // recalculate all the shares, so don't bother just
  // updating this client.
  if (!dirty) {
    update(name);
  }
}


void DRFSorter::update(
    const string& name,
    const SlaveID& slaveId,
    const cpp::Resources& oldAllocation,
    const cpp::Resources& newAllocation)
{
  CHECK(contains(name));

  // TODO(bmahler): Check invariants between old and new allocations.
  // Namely, the roles and quantities of resources should be the same!
  // Otherwise, we need to ensure we re-calculate the shares, as
  // is being currently done, for safety.

  CHECK(total_.resources[slaveId].contains(oldAllocation));
  CHECK(total_.scalars.contains(oldAllocation.scalars()));

  total_.resources[slaveId] -= oldAllocation;
  total_.resources[slaveId] += newAllocation;

  total_.scalars -= oldAllocation.scalars();
  total_.scalars += newAllocation.scalars();

  CHECK(allocations[name].resources[slaveId].contains(oldAllocation));
  CHECK(allocations[name].scalars.contains(oldAllocation.scalars()));

  allocations[name].resources[slaveId] -= oldAllocation;
  allocations[name].resources[slaveId] += newAllocation;

  allocations[name].scalars -= oldAllocation.scalars();
  allocations[name].scalars += newAllocation.scalars();

  // Just assume the total has changed, per the TODO above.
  dirty = true;
}


const hashmap<SlaveID, cpp::Resources>& DRFSorter::allocation(const string& name)
{
  CHECK(contains(name));

  return allocations[name].resources;
}


const cpp::Resources& DRFSorter::allocationScalars(const string& name)
{
  CHECK(contains(name));

  return allocations[name].scalars;
}


hashmap<string, cpp::Resources> DRFSorter::allocation(const SlaveID& slaveId)
{
  // TODO(jmlvanre): We can index the allocation by slaveId to make this faster.
  // It is a tradeoff between speed vs. memory. For now we use existing data
  // structures.

  hashmap<string, cpp::Resources> result;

  foreachpair (const string& name, const Allocation& allocation, allocations) {
    if (allocation.resources.contains(slaveId)) {
      // It is safe to use `at()` here because we've just checked the existence
      // of the key. This avoid un-necessary copies.
      result.emplace(name, allocation.resources.at(slaveId));
    }
  }

  return result;
}


cpp::Resources DRFSorter::allocation(const string& name, const SlaveID& slaveId)
{
  CHECK(contains(name));

  if (allocations[name].resources.contains(slaveId)) {
    return allocations[name].resources[slaveId];
  }

  return cpp::Resources();
}


const hashmap<SlaveID, cpp::Resources>& DRFSorter::total() const
{
  return total_.resources;
}


const cpp::Resources& DRFSorter::totalScalars() const
{
  return total_.scalars;
}


void DRFSorter::unallocated(
    const string& name,
    const SlaveID& slaveId,
    const cpp::Resources& resources)
{
  allocations[name].resources[slaveId] -= resources;
  allocations[name].scalars -= resources.scalars();

  if (allocations[name].resources[slaveId].empty()) {
    allocations[name].resources.erase(slaveId);
  }

  if (!dirty) {
    update(name);
  }
}


void DRFSorter::add(const SlaveID& slaveId, const cpp::Resources& resources)
{
  if (!resources.empty()) {
    total_.resources[slaveId] += resources;
    total_.scalars += resources.scalars();

    // We have to recalculate all shares when the total resources
    // change, but we put it off until sort is called so that if
    // something else changes before the next allocation we don't
    // recalculate everything twice.
    dirty = true;
  }
}


void DRFSorter::remove(const SlaveID& slaveId, const cpp::Resources& resources)
{
  if (!resources.empty()) {
    CHECK(total_.resources.contains(slaveId));

    total_.resources[slaveId] -= resources;
    total_.scalars -= resources.scalars();

    if (total_.resources[slaveId].empty()) {
      total_.resources.erase(slaveId);
    }

    dirty = true;
  }
}


void DRFSorter::update(const SlaveID& slaveId, const cpp::Resources& resources)
{
  CHECK(total_.scalars.contains(total_.resources[slaveId].scalars()));

  total_.scalars -= total_.resources[slaveId].scalars();
  total_.scalars += resources.scalars();

  total_.resources[slaveId] = resources;

  if (total_.resources[slaveId].empty()) {
    total_.resources.erase(slaveId);
  }

  dirty = true;
}


list<string> DRFSorter::sort()
{
  if (dirty) {
    set<Client, DRFComparator> temp;

    set<Client, DRFComparator>::iterator it;
    for (it = clients.begin(); it != clients.end(); it++) {
      Client client(*it);

      // Update the 'share' to get proper sorting.
      client.share = calculateShare(client.name);

      temp.insert(client);
    }

    clients = temp;
  }

  list<string> result;

  set<Client, DRFComparator>::iterator it;
  for (it = clients.begin(); it != clients.end(); it++) {
    result.push_back((*it).name);
  }

  return result;
}


bool DRFSorter::contains(const string& name)
{
  return allocations.contains(name);
}


int DRFSorter::count()
{
  return allocations.size();
}


void DRFSorter::update(const string& name)
{
  set<Client, DRFComparator>::iterator it = find(name);

  if (it != clients.end()) {
    Client client(*it);

    // Update the 'share' to get proper sorting.
    client.share = calculateShare(client.name);

    // Remove and reinsert it to update the ordering appropriately.
    clients.erase(it);
    clients.insert(client);
  }
}


double DRFSorter::calculateShare(const string& name)
{
  double share = 0.0;

  // TODO(benh): This implementation of "dominant resource fairness"
  // currently does not take into account resources that are not
  // scalars.

  foreach (const string& scalar, total_.scalars.names()) {
    // We collect the scalar accumulated total value from the
    // `Resources` object.
    //
    // NOTE: Scalar resources may be spread across multiple
    // 'Resource' objects. E.g. persistent volumes.
    Option<Value::Scalar> __total = total_.scalars.get<Value::Scalar>(scalar);
    CHECK_SOME(__total);
    const double _total = __total.get().value();

    if (_total > 0.0) {
      double allocation = 0.0;

      // We collect the scalar accumulated allocation value from the
      // `Resources` object.
      //
      // NOTE: Scalar resources may be spread across multiple
      // 'Resource' objects. E.g. persistent volumes.
      Option<Value::Scalar> _allocation =
        allocations[name].scalars.get<Value::Scalar>(scalar);

      if (_allocation.isSome()) {
        allocation = _allocation.get().value();
      }

      share = std::max(share, allocation / _total);
    }
  }

  return share / weights[name];
}


set<Client, DRFComparator>::iterator DRFSorter::find(const string& name)
{
  set<Client, DRFComparator>::iterator it;
  for (it = clients.begin(); it != clients.end(); it++) {
    if (name == (*it).name) {
      break;
    }
  }

  return it;
}

} // namespace allocator {
} // namespace master {
} // namespace internal {
} // namespace mesos {
