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

#include <stdint.h>

#include <vector>

#include <mesos/resources.hpp>
#include <mesos/values.hpp>

#include <process/defer.hpp>

#include "linux/cgroups.hpp"

#include "slave/flags.hpp"

#include "slave/containerizer/isolators/cgroups/blkio.hpp"

using namespace process;

using std::list;
using std::string;
using std::vector;

namespace mesos {
namespace internal {
namespace slave {


template<class T>
static Future<Option<T> > none() { return None(); }

CgroupsBlkIOIsolatorProcess::CgroupsBlkIOIsolatorProcess(
    const Flags& _flags,
    const std::string& _hierarchy)
  : flags(_flags),
    hierarchy(_hierarchy) {}


CgroupsBlkIOIsolatorProcess::~CgroupsBlkIOIsolatorProcess() {}


Try<Isolator*> CgroupsBlkIOIsolatorProcess::create(const Flags& flags)
{
  Try<string> hierarchy = cgroups::prepare(
      flags.cgroups_hierarchy, "blkio", flags.cgroups_root);
  if (hierarchy.isError()) {
    return Error("Failed to create blkio isolator: " + hierarchy.error());
  }
  process::Owned<IsolatorProcess> process(
      new CgroupsBlkIOIsolatorProcess(flags, hierarchy.get()));

  return new Isolator(process);
}


Future<Nothing> CgroupsBlkIOIsolatorProcess::recover(
    const list<state::RunState>& states)
{
  hashset<string> cgroups;

  foreach (const state::RunState& state, states) {
    if (state.id.isNone()) {
      foreachvalue (Info* info, infos) {
        delete info;
      }
      infos.clear();
      return Failure("ContainerID is required to recover");
    }

    const ContainerID& containerId = state.id.get();
    const string cgroup = path::join(flags.cgroups_root, containerId.value());

    Try<bool> exists = cgroups::exists(hierarchy, cgroup);
    if (exists.isError()) {
      foreachvalue (Info* info, infos) {
        delete info;
      }
      infos.clear();
      return Failure("Failed to check cgroup for container '" +
      stringify(containerId) + "'");
    }

    if (!exists.get()) {
      VLOG(1) << "Couldn't find cgroup for container " << containerId;
      // This may occur if the executor has exited and the isolator
      // has destroyed the cgroup but the slave dies before noticing
      // this. This will be detected when the containerizer tries to
      // monitor the executor's pid.
      continue;
    }

    infos[containerId] = new Info(containerId, cgroup);
    cgroups.insert(cgroup);
  }

  Try<vector<string> > orphans = cgroups::get(hierarchy, flags.cgroups_root);
  if (orphans.isError()) {
    foreachvalue (Info* info, infos) {
      delete info;
    }
    infos.clear();
    return Failure(orphans.error());
  }

  foreach (const string& orphan, orphans.get()) {
    // Ignore the slave cgroup (see the --slave_subsystems flag).
    // TODO(idownes): Remove this when the cgroups layout is updated,
    // see MESOS-1185.
    if (orphan == path::join(flags.cgroups_root, "slave")) {
      continue;
    }

    if (!cgroups.contains(orphan)) {
      LOG(INFO) << "Removing orphaned cgroup '" << orphan << "'";
      // We don't wait on the destroy as we don't want to block recovery.
      cgroups::destroy(hierarchy, orphan, cgroups::DESTROY_TIMEOUT);
    }
  }

  return Nothing();
}


Future<Option<CommandInfo> > CgroupsBlkIOIsolatorProcess::prepare(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo)
{
  if (infos.contains(containerId)) {
    return Failure("Container has already been prepared");
  }

  LOG(INFO) << "Preparing blkio cgroup for " << containerId;

  Info* info = new Info(
      containerId,
      path::join(flags.cgroups_root, containerId.value()));

  infos[containerId] = CHECK_NOTNULL(info);

  // Create a cgroup for this container.
  Try<bool> exists = cgroups::exists(hierarchy, info->cgroup);

  if (exists.isError()) {
    return Failure("Failed to prepare isolator: " + exists.error());
  }

  if (exists.get()) {
    return Failure("Failed to prepare isolator: cgroup already exists");
  }

  Try<Nothing> create = cgroups::create(hierarchy, info->cgroup);
  if (create.isError()) {
    return Failure("Failed to prepare isolator: " + create.error());
  }

  return update(containerId, executorInfo.resources())
    .then(lambda::bind(none<CommandInfo>));
}


Future<Nothing> CgroupsBlkIOIsolatorProcess::isolate(
    const ContainerID& containerId,
    pid_t pid)
{
  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  Info* info = CHECK_NOTNULL(infos[containerId]);

  if (info->pid.isSome()) {
    return Failure("Pid " + stringify(info->pid.get()) + " for container " +
        stringify(info->containerId) + " has already been isolated");
  }
  info->pid = pid;

  Try<Nothing> assign = cgroups::assign(hierarchy, info->cgroup, pid);
  if (assign.isError()) {
    return Failure("Failed to assign container '" +
                   stringify(info->containerId) + "' to its own cgroup '" +
                   path::join(hierarchy, info->cgroup) +
                   "' : " + assign.error());
  }

  return Nothing();
}


Future<Limitation> CgroupsBlkIOIsolatorProcess::watch(
    const ContainerID& containerId)
{
  // TODO(preilly): Deal with limitations being met.
  return Failure("blkio watch not implemented");
}


Future<Nothing> CgroupsBlkIOIsolatorProcess::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  if (resources.blk_read_bps().isNone() && resources.blk_read_iops().isNone() &&
    resources.blk_write_bps().isNone() && resources.blk_write_iops().isNone()
  ) {
    return Failure("No blkio resource given");
  }

  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  Info* info = CHECK_NOTNULL(infos[containerId]);

  // New limit.
  Option<Bytes> r_bps = resources.blk_read_bps();
  Option<Bytes> w_bps = resources.blk_write_bps();
  Option<uint64_t> r_iops = resources.blk_read_iops();
  Option<uint64_t> w_iops = resources.blk_write_iops();

  if (r_bps.isSome()) {
    Try<Nothing> write = cgroups::blkio::limit_in_bytes(hierarchy, info->cgroup,
                                                        "read", r_bps.get());
    if (write.isError()) {
      return Failure(
        "Failed to set 'blkio.throttle.read_bps_device': " + write.error());
    }
    LOG(INFO) << "Updated 'blkio.throttle.read_bps_device' to " << r_bps.get()
    << " for container " << containerId;
  }
  if (w_bps.isSome()) {
    Try<Nothing> write = cgroups::blkio::limit_in_bytes(hierarchy, info->cgroup,
                                                        "write", w_bps.get());
    if (write.isError()) {
      return Failure(
        "Failed to set 'blkio.throttle.write_bps_device': " + write.error());
    }
    LOG(INFO) << "Updated 'blkio.throttle.write_bps_device' to " << w_bps.get()
    << " for container " << containerId;
  }
  if (r_iops.isSome()) {
    Try<Nothing> write = cgroups::blkio::limit_in_iops(hierarchy, info->cgroup,
                                                       "read", r_iops.get());
    if (write.isError()) {
      return Failure(
        "Failed to set 'blkio.throttle.read_iops_device': " + write.error());
    }
    LOG(INFO) << "Updated 'blkio.throttle.read_iops_device' to " << r_iops.get()
    << " for container " << containerId;
  }
  if (w_iops.isSome()) {
    Try<Nothing> write = cgroups::blkio::limit_in_iops(hierarchy, info->cgroup,
                                                       "write", w_iops.get());
    if (write.isError()) {
      return Failure(
        "Failed to set 'blkio.throttle.write_iops_device': " + write.error());
    }
    LOG(INFO) << "Updated 'blkio.throttle.write_iops_device' to "
    << w_iops.get() << " for container " << containerId;
  }

  return Nothing();
}


Future<ResourceStatistics> CgroupsBlkIOIsolatorProcess::usage(
    const ContainerID& containerId)
{
  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  Info* info = CHECK_NOTNULL(infos[containerId]);

  ResourceStatistics result;

  Try<std::pair<uint64_t, uint64_t> > iops = cgroups::blkio::statAggregates(
      hierarchy,
      info->cgroup,
      "blkio.throttle.io_serviced");
  if (iops.isError()) {
    return Failure("Failed to parse blkio.throttle.io_serviced: " +
      iops.error());
  }
  result.set_disk_read_total_iops(iops.get().first);
  result.set_disk_write_total_iops(iops.get().second);

  Try<std::pair<uint64_t, uint64_t> > bytes = cgroups::blkio::statAggregates(
      hierarchy,
      info->cgroup,
      "blkio.throttle.io_service_bytes");
  if (bytes.isError()) {
    return Failure("Failed to parse blkio.throttle.io_service_bytes: " +
      bytes.error());
  }
  result.set_disk_read_total_bytes(bytes.get().first);
  result.set_disk_write_total_bytes(bytes.get().second);

  return result;
}


Future<Nothing> CgroupsBlkIOIsolatorProcess::cleanup(
    const ContainerID& containerId)
{
  // Multiple calls may occur during test clean up.
  if (!infos.contains(containerId)) {
    VLOG(1) << "Ignoring cleanup request for unknown container: "
            << containerId;
    return Nothing();
  }

  Info* info = CHECK_NOTNULL(infos[containerId]);

  return cgroups::destroy(hierarchy, info->cgroup, cgroups::DESTROY_TIMEOUT)
    .onAny(defer(PID<CgroupsBlkIOIsolatorProcess>(this),
                 &CgroupsBlkIOIsolatorProcess::_cleanup,
                 containerId,
                 lambda::_1));
}


Future<Nothing> CgroupsBlkIOIsolatorProcess::_cleanup(
    const ContainerID& containerId,
    const process::Future<Nothing>& future)
{
  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  CHECK_NOTNULL(infos[containerId]);

  if (!future.isReady()) {
    return Failure("Failed to clean up container " + stringify(containerId) +
                   " : " + (future.isFailed() ? future.failure()
                                              : "discarded"));
  }

  delete infos[containerId];
  infos.erase(containerId);

  return Nothing();
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {
