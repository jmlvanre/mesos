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

#include <unistd.h>

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include <mesos/maintenance/maintenance.hpp>
#include <mesos/scheduler/scheduler.hpp>

#include <process/future.hpp>
#include <process/http.hpp>
#include <process/pid.hpp>

#include <stout/json.hpp>
#include <stout/net.hpp>
#include <stout/option.hpp>
#include <stout/protobuf.hpp>
#include <stout/try.hpp>
#include <stout/strings.hpp>
#include <stout/stringify.hpp>

#include "master/master.hpp"

#include "slave/flags.hpp"

#include "tests/containerizer.hpp"
#include "tests/mesos.hpp"
#include "tests/utils.hpp"

using mesos::internal::master::Master;

using mesos::internal::slave::Slave;

using process::Future;
using process::PID;

using process::http::BadRequest;
using process::http::OK;
using process::http::Response;

using std::string;
using std::vector;

using testing::DoAll;

namespace mesos {
namespace internal {
namespace tests {

class MasterMaintenanceTest : public MesosTest {};


// Posts valid and invalid schedules to the maintenance schedule endpoint.
TEST_F(MasterMaintenanceTest, UpdateSchedule)
{
  // Set up a master.
  Try<PID<Master>> master = StartMaster();
  ASSERT_SOME(master);

  // Header for all the POST's in this test.
  hashmap<string, string> headers;
  headers["Content-Type"] = "application/json";

  // JSON machines used in this test.
  JSON::Object machine1;
  machine1.values["hostname"] = "Machine1";

  JSON::Object machine2;
  machine2.values["ip"] = "0.0.0.2";

  JSON::Object badMachine;

  // JSON arrays of machines used in this test.
  JSON::Array machines1;
  machines1.values.push_back(machine1);

  JSON::Array emptyMachines;

  JSON::Array badMachines;
  badMachines.values.push_back(badMachine);

  JSON::Array machines12;
  machines12.values.push_back(machine2);
  machines12.values.push_back(machine1);

  // JSON windows used in this test.
  JSON::Object window1;
  window1.values["machines"] = machines1;

  JSON::Object emptyWindow;
  emptyWindow.values["machines"] = emptyMachines;

  JSON::Object badWindow;
  badWindow.values["machines"] = badMachines;

  JSON::Object window12;
  window12.values["machines"] = machines12;

  // JSON schedules used in this test.
  JSON::Object validSchedule;
  JSON::Array validWindows;
  validWindows.values.push_back(window1);
  validSchedule.values["windows"] = validWindows;

  JSON::Object badScheduleWithEmptyWindows;
  JSON::Array emptyWindows;
  emptyWindows.values.push_back(emptyWindow);
  badScheduleWithEmptyWindows.values["windows"] = emptyWindows;

  JSON::Object badScheduleWithDuplicateMachines;
  JSON::Array duplicateWindows;
  duplicateWindows.values.push_back(window1);
  duplicateWindows.values.push_back(window1);
  badScheduleWithDuplicateMachines.values["windows"] = duplicateWindows;

  JSON::Object badScheduleWithBadMachines;
  JSON::Array badWindows;
  badWindows.values.push_back(badWindow);
  badScheduleWithBadMachines.values["windows"] = badWindows;

  JSON::Object validScheduleWithMoreMachines;
  JSON::Array validWindowsWithTwoMachines;
  validWindowsWithTwoMachines.values.push_back(window12);
  validScheduleWithMoreMachines.values["windows"] = validWindowsWithTwoMachines;

  JSON::Object validEmptySchedule;

  // -- Start of the test. --

  // Post a valid schedule.
  Future<Response> response =
    process::http::post(master.get(),
      "maintenance.schedule",
      headers,
      stringify(validSchedule));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Get the maintenance schedule.
  response =
    process::http::get(master.get(),
      "maintenance.schedule");
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Make sure the hostname was lowercased.
  Try<JSON::Object> masterBlob =
    JSON::parse<JSON::Object>(response.get().body);
  Try<mesos::maintenance::Schedule> masterSchedule =
    ::protobuf::parse<mesos::maintenance::Schedule>(masterBlob.get());
  ASSERT_EQ(1, masterSchedule.get().windows().size());
  ASSERT_EQ(1, masterSchedule.get().windows(0).machines().size());
  ASSERT_EQ("machine1", masterSchedule.get().windows(0).machines(0).hostname());

  // Try to replace with an invalid schedule.
  response =
    process::http::post(master.get(),
      "maintenance.schedule",
      headers,
      stringify(badScheduleWithEmptyWindows));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  // Try to replace with another invalid schedule.
  response =
    process::http::post(master.get(),
      "maintenance.schedule",
      headers,
      stringify(badScheduleWithDuplicateMachines));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  // Try to replace with yet another invalid schedule.
  response =
    process::http::post(master.get(),
      "maintenance.schedule",
      headers,
      stringify(badScheduleWithBadMachines));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  // Post a valid extended schedule.
  response =
    process::http::post(master.get(),
      "maintenance.schedule",
      headers,
      stringify(validScheduleWithMoreMachines));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Delete the schedule (via an empty schedule).
  response =
    process::http::post(master.get(),
      "maintenance.schedule",
      headers,
      stringify(validEmptySchedule));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);
}


// Posts valid and invalid machines to the maintenance start endpoint.
TEST_F(MasterMaintenanceTest, DeactivateMachines)
{
  // Set up a master.
  Try<PID<Master>> master = StartMaster();
  ASSERT_SOME(master);

  // Header for all the POST's in this test.
  hashmap<string, string> headers;
  headers["Content-Type"] = "application/json";

  // JSON machines used in this test.
  JSON::Object machine1;
  machine1.values["hostname"] = "Machine1";

  JSON::Object machine2;
  machine2.values["ip"] = "0.0.0.2";

  JSON::Object badMachine;

  // JSON arrays of machines used in this test.
  JSON::Array machines12;
  machines12.values.push_back(machine2);
  machines12.values.push_back(machine1);

  JSON::Array emptyMachines;

  JSON::Array badMachines;
  badMachines.values.push_back(badMachine);

  JSON::Array machines1;
  machines1.values.push_back(machine1);

  JSON::Array machines2;
  machines2.values.push_back(machine2);

  // JSON windows (or MachineInfos) used in this test.
  JSON::Object window12;
  window12.values["machines"] = machines12;

  JSON::Object emptyWindow;

  JSON::Object badWindow;
  badWindow.values["machines"] = badMachines;

  JSON::Object window1;
  window1.values["machines"] = machines1;

  JSON::Object window2;
  window2.values["machines"] = machines2;

  // JSON schedules used in this test.
  JSON::Object validSchedule12;
  JSON::Array validWindows12;
  validWindows12.values.push_back(window12);
  validSchedule12.values["windows"] = validWindows12;

  // -- Start of the test. --

  // Try to start maintenance on an unscheduled machine.
  Future<Response> response =
    process::http::post(master.get(),
      "maintenance.start",
      headers,
      stringify(window12));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  // Try an empty list.
  response =
    process::http::post(master.get(),
      "maintenance.start",
      headers,
      stringify(emptyWindow));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  // Try an empty machine.
  response =
    process::http::post(master.get(),
      "maintenance.start",
      headers,
      stringify(badWindow));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  // Post a valid schedule with two machines.
  response =
    process::http::post(master.get(),
      "maintenance.schedule",
      headers,
      stringify(validSchedule12));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Deactivate machine1.
  response =
    process::http::post(master.get(),
      "maintenance.start",
      headers,
      stringify(window1));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Fail to deactivate machine1 again.
  response =
    process::http::post(master.get(),
      "maintenance.start",
      headers,
      stringify(window1));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  // Fail to deactivate machine1 and machine2.
  response =
    process::http::post(master.get(),
      "maintenance.start",
      headers,
      stringify(window12));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  // Deactivate machine2.
  response =
    process::http::post(master.get(),
      "maintenance.start",
      headers,
      stringify(window2));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);
}


// Posts valid and invalid machines to the maintenance stop endpoint.
TEST_F(MasterMaintenanceTest, ReactivateMachines)
{
  // Set up a master.
  Try<PID<Master>> master = StartMaster();
  ASSERT_SOME(master);

  // Header for all the POST's in this test.
  hashmap<string, string> headers;
  headers["Content-Type"] = "application/json";

  // JSON machines used in this test.
  JSON::Object machine1;
  machine1.values["hostname"] = "Machine1";

  JSON::Object machine2;
  machine2.values["ip"] = "0.0.0.2";

  JSON::Object machine3;
  machine3.values["hostname"] = "Machine3";
  machine3.values["ip"] = "0.0.0.3";

  // JSON arrays of machines used in this test.
  JSON::Array machines12;
  machines12.values.push_back(machine2);
  machines12.values.push_back(machine1);

  JSON::Array machines3;
  machines3.values.push_back(machine3);

  // JSON windows (or MachineInfos) used in this test.
  JSON::Object window12;
  window12.values["machines"] = machines12;

  JSON::Object window3;
  window3.values["machines"] = machines3;

  // JSON schedule used in this test.
  JSON::Object validSchedule123;
  JSON::Array validWindows123;
  validWindows123.values.push_back(window12);
  validWindows123.values.push_back(window3);
  validSchedule123.values["windows"] = validWindows123;

  // -- Start of the test. --

  // Try to stop maintenance on an unscheduled machine.
  Future<Response> response =
    process::http::post(master.get(),
      "maintenance.stop",
      headers,
      stringify(window12));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  // Post a valid schedule with three machines.
  response =
    process::http::post(master.get(),
      "maintenance.schedule",
      headers,
      stringify(validSchedule123));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Try to stop maintenance on a Draining machine.
  response =
    process::http::post(master.get(),
      "maintenance.stop",
      headers,
      stringify(window12));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  // Deactivate machine3.
  response =
    process::http::post(master.get(),
      "maintenance.start",
      headers,
      stringify(window3));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Reactivate machine3.
  response =
    process::http::post(master.get(),
      "maintenance.stop",
      headers,
      stringify(window3));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Get the maintenance schedule.
  response =
    process::http::get(master.get(),
      "maintenance.schedule");
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Check that only one maintenance window remains.
  Try<JSON::Object> masterBlob =
    JSON::parse<JSON::Object>(response.get().body);
  Try<mesos::maintenance::Schedule> masterSchedule =
    ::protobuf::parse<mesos::maintenance::Schedule>(masterBlob.get());
  ASSERT_EQ(1, masterSchedule.get().windows().size());
  ASSERT_EQ(2, masterSchedule.get().windows(0).machines().size());

  // Deactivate the other machines.
  response =
    process::http::post(master.get(),
      "maintenance.start",
      headers,
      stringify(window12));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Reactivate the other machines.
  response =
    process::http::post(master.get(),
      "maintenance.stop",
      headers,
      stringify(window12));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Get the maintenance schedule again.
  response =
    process::http::get(master.get(),
      "maintenance.schedule");
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  // Check that the schedule is empty.
  masterBlob = JSON::parse<JSON::Object>(response.get().body);
  masterSchedule =
    ::protobuf::parse<mesos::maintenance::Schedule>(masterBlob.get());
  ASSERT_EQ(0, masterSchedule.get().windows().size());
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
