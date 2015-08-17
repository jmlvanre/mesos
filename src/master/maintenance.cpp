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

#include <mesos/type_utils.hpp>

#include <stout/duration.hpp>
#include <stout/error.hpp>
#include <stout/hashset.hpp>
#include <stout/ip.hpp>
#include <stout/nothing.hpp>
#include <stout/strings.hpp>

#include <process/time.hpp>

#include <mesos/maintenance/maintenance.hpp>

#include "logging/logging.hpp"

#include "master/maintenance.hpp"

namespace mesos {
namespace internal {
namespace master {
namespace maintenance {

using namespace mesos::maintenance;

UpdateSchedule::UpdateSchedule(
    const maintenance::Schedule& _schedule)
  : schedule(_schedule) {}


Try<bool> UpdateSchedule::perform(
    Registry* registry,
    hashset<SlaveID>* slaveIDs,
    bool strict)
{
  // Put the machines in the existing schedule into a set.
  hashset<MachineInfo> existing;
  foreach (const maintenance::Schedule& agenda, registry->schedules()) {
    foreach (const maintenance::Window& window, agenda.windows()) {
      foreach (const MachineInfo& machine, window.machines()) {
        existing.insert(machine);
      }
    }
  }

  // Put the machines in the updated schedule into a set.
  hashset<MachineInfo> updated;
  foreach (const maintenance::Window& window, schedule.windows()) {
    foreach (const MachineInfo& machine, window.machines()) {
      updated.insert(machine);
    }
  }

  // This operation overwrites the existing schedules with a new one.
  // At the same time, every machine in a schedule must have an
  // associated MaintenanceStatus in the registry.

  // TODO(josephw): allow more than one schedule.

  // Create MaintenanceStatuses for each new machine.
  foreach (const maintenance::Window& window, schedule.windows()) {
    foreach (const MachineInfo& machine, window.machines()) {
      if (existing.contains(machine)) {
        continue;
      }

      // Each newly scheduled machine starts in Draining mode.
      Registry::MaintenanceStatus* status = registry->add_statuses();
      status->mutable_id()->CopyFrom(machine);
      status->set_mode(maintenance::DRAINING);
    }
  }

  // Delete MaintenanceStatuses for each removed machine.
  for (int i = registry->statuses().size() - 1; i >= 0; i--) {
    const MachineInfo& machine = registry->statuses(i).id();
    if (updated.contains(machine)) {
      continue;
    }

    registry->mutable_statuses()->DeleteSubrange(i, 1);
  }

  // Replace the old schedule with the new schedule.
  registry->clear_schedules();
  registry->add_schedules()->CopyFrom(schedule);

  return true; // Mutation.
}


StartMaintenance::StartMaintenance(
    const MachineInfos& _machines)
{
  foreach (const MachineInfo& machine, _machines.machines()) {
    machines.insert(machine);
  }
}


Try<bool> StartMaintenance::perform(
    Registry* registry,
    hashset<SlaveID>* slaveIDs,
    bool strict)
{
  // Flip the mode of all targeted machines.
  bool changed = false;
  for (int i = 0; i < registry->statuses().size(); i++) {
    const Registry::MaintenanceStatus& status = registry->statuses(i);

    if (machines.contains(status.id())) {
      // Flip the mode.
      registry->mutable_statuses()->Mutable(i)
        ->set_mode(maintenance::DEACTIVATED);

      changed = true; // Mutation.
    }
  }

  return changed;
}

namespace validation {

Try<Nothing> schedule(
    maintenance::Schedule& agenda,
    const hashmap<MachineInfo, MaintenanceInfo>& infos)
{
  hashset<MachineInfo> updated;
  for (int i = 0; i < agenda.windows().size(); i++) {
    const maintenance::Window& window = agenda.windows(i);

    // Check that each window has at least one machine.
    if (window.machines().size() == 0) {
      return Error("List of machines missing from the window");
    }

    // Check unavailability for numbers.
    Try<Nothing> unavailability =
        maintenance::validation::unavailability(window.unavailability());
    if (unavailability.isError()) {
      return Error(unavailability.error());
    }

    // Put the machines in the updated schedule into a set.
    for (int j = 0; j < window.machines().size(); j++) {
      const MachineInfo& machine = window.machines(j);

      // Validate the single machine.
      // This has the desired side effect of lowercasing the hostname.
      Try<Nothing> validMachine =
        validation::machine(agenda.mutable_windows(i)->mutable_machines(j));
      if (validMachine.isError()) {
        return Error(validMachine.error());
      }

      // Check machine uniqueness.
      if (updated.contains(machine)) {
        return Error("Machine " + machine.DebugString() +
            " appears more than once in the schedule");
      }

      updated.insert(machine);
    }
  }

  // Validate that all machines being removed from the schedule are
  // *not* in Deactivated mode.
  foreachpair (const MachineInfo& key, const MaintenanceInfo& value, infos) {
    if (value.mode == maintenance::DEACTIVATED && !updated.contains(key)) {
      return Error("Machine " + key.DebugString() +
        " is deactivated and cannot be removed from the schedule");
    }
  }

  return Nothing();
}


Try<Nothing> unavailability(const Unavailability& interval)
{
  const double start = interval.start();
  const double duration = interval.duration();

  // Check the bounds of the interval.
  if (!std::isfinite(start) ||
      start < 0 ||
      !std::isfinite(duration) ||
      duration < 0) {
    return Error("Unavailability start and duration are not positive numbers");
  }

  // Try parsing the inverval into Time and Duration objects.
  Try<process::Time> timed = process::Time::create(start);
  if (timed.isError()) {
    return Error(timed.error());
  }

  Try<Duration> durationed = Duration::create(duration);
  if (durationed.isError()) {
    return Error(durationed.error());
  }


  return Nothing();
}


Try<Nothing> machines(MachineInfos& machines)
{
  if (machines.machines().size() <= 0) {
    return Error("List of machines is empty.");
  }

  // Verify that the machine has at least one non-empty field value.
  hashset<MachineInfo> uniques;
  for (int i = 0; i < machines.machines().size(); i++) {
    const MachineInfo& machine = machines.machines(i);

    // Validate the single machine.
    // This has the desired side effect of lowercasing the hostname.
    Try<Nothing> validMachine =
      validation::machine(machines.mutable_machines(i));
    if (validMachine.isError()) {
      return Error(validMachine.error());
    }

    // Check machine uniqueness.
    if (uniques.contains(machine)) {
      return Error("Machine " + machine.DebugString() +
          " appears more than once in the schedule");
    }

    uniques.insert(machine);
  }

  return Nothing();
}


Try<Nothing> machine(MachineInfo* machine)
{
  // Lowercase the machine's hostname
  if (!machine->hostname().empty()) {
    machine->set_hostname(strings::lower(machine->hostname()));
  }

  // Verify that the machine has at least one non-empty field value.
  if (machine->hostname().empty() && machine->ip().empty()) {
    return Error("Both \"hostname\" and \"ip\" for a machine are empty");
  }

  // Validate the IP field.
  if (!machine->ip().empty()) {
    Try<net::IP> ip = net::IP::parse(machine->ip(), AF_INET);
    if (ip.isError()) {
      return Error(ip.error());
    }
  }

  return Nothing();
}

} // namespace validation {
} // namespace maintenance {
} // namespace master {
} // namespace internal {
} // namespace mesos {
