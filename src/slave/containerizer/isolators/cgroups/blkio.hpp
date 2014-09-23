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

#ifndef __BLKIO_ISOLATOR_HPP__
#define __BLKIO_ISOLATOR_HPP__

#include <string>

#include <stout/hashmap.hpp>

#include "slave/containerizer/isolator.hpp"

#include "slave/containerizer/isolators/cgroups/constants.hpp"

namespace mesos {
namespace internal {
namespace slave {

// Use the Linux blkio cgroup subsystem for disk isolation.
// TODO(preilly): Further explanation.
class CgroupsBlkIOIsolatorProcess : public IsolatorProcess
{
public:
  static Try<Isolator*> create(const Flags& flags);

  virtual ~CgroupsBlkIOIsolatorProcess();

  virtual process::Future<Nothing> recover(
      const std::list<state::RunState>& states);

  virtual process::Future<Option<CommandInfo> > prepare(
      const ContainerID& containerId,
      const ExecutorInfo& executorInfo);

  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid);

  virtual process::Future<Limitation> watch(
      const ContainerID& containerId);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<Nothing> cleanup(
      const ContainerID& containerId);

private:
  CgroupsBlkIOIsolatorProcess(
      const Flags& flags,
      const std::string& hierarchy);

  virtual process::Future<Nothing> _cleanup(
      const ContainerID& containerId,
      const process::Future<Nothing>& future);

  struct Info
  {
    Info(const ContainerID& _containerId, const std::string& _cgroup)
      : containerId(_containerId), cgroup(_cgroup) {}

    const ContainerID containerId;
    const std::string cgroup;
    Option<pid_t> pid;

    // TODO(preilly): Make a representation of limits.
  };

  const Flags flags;

  // The path to the cgroups subsystem hierarchy root.
  const std::string hierarchy;

  // TODO(bmahler): Use Owned<Info>.
  hashmap<ContainerID, Info*> infos;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __BLKIO_ISOLATOR_HPP__
