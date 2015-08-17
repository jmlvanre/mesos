/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __MESOS_MASTER_MAINTENANCE_HPP__
#define __MESOS_MASTER_MAINTENANCE_HPP__

#include <stout/hashset.hpp>
#include <stout/nothing.hpp>
#include <stout/try.hpp>

#include <mesos/mesos.hpp>
#include <mesos/maintenance/maintenance.hpp>

#include "master/registrar.hpp"
#include "master/registry.hpp"

namespace mesos {
namespace internal {
namespace master {
namespace maintenance {

// Updates the maintanence schedule of the cluster.  This transitions machines
// between Normal and Draining modes only.  The given schedule must only add
// valid machines and remove non-Deactivated machines.
// TODO(josephw): allow more than one schedule.
class UpdateSchedule : public Operation
{
public:
  explicit UpdateSchedule(
      const mesos::maintenance::Schedule& _schedule);

protected:
  Try<bool> perform(
      Registry* registry,
      hashset<SlaveID>* slaveIDs,
      bool strict);

private:
  const mesos::maintenance::Schedule schedule;
};


// Transitions a group of machines from Draining mode into
// Deactivated mode.  All machines must be part of a maintenance
// schedule prior to executing this operation.
class StartMaintenance : public Operation
{
public:
  explicit StartMaintenance(
      const MachineInfos& _machines);

protected:
  Try<bool> perform(
      Registry* registry,
      hashset<SlaveID>* slaveIDs,
      bool strict);

private:
  hashset<MachineInfo> machines;
};

namespace validation {

// Performs the following checks on the new maintenance schedule:
// * Each window in the new schedule has at least one machine.
// * All unavailabilities contain valid numbers.
// * Each machine appears in the schedule once and only once.
// * All currently Deactivated machines are present in the schedule.
// * All checks in the "machine" method below.
Try<Nothing> schedule(
    mesos::maintenance::Schedule& agenda,
    const hashmap<MachineInfo, MaintenanceInfo>& infos);


// Checks that the start and duration of the object are non-negative numbers.
Try<Nothing> unavailability(
    const Unavailability& interval);


// Performs the following checks on a list of machines:
// * Each machine appears in the list once and only once.
// * The list is non-empty.
// * All checks in the "machine" method below.
Try<Nothing> machines(MachineInfos& machines);


// Performs the following checks on a single machine:
// * ! Enforces the hostname into lowercase. !
// * The machine has at least a hostname or IP.
// * IP is correctly formed.
Try<Nothing> machine(MachineInfo* machine);

} // namespace validation {
} // namespace maintenance {
} // namespace master {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_MASTER_MAINTENANCE_HPP__
