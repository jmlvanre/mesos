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

#include <gtest/gtest.h>

#include <gmock/gmock.h>

#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_set>

#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/process.hpp>

using namespace process;

using std::condition_variable;
using std::function;
using std::istringstream;
using std::lock_guard;
using std::mutex;
using std::ostringstream;
using std::string;
using std::thread;
using std::unique_lock;
using std::unordered_set;
using std::vector;

int main(int argc, char** argv)
{
  // Initialize Google Mock/Test.
  testing::InitGoogleMock(&argc, argv);

  // Add the libprocess test event listeners.
  ::testing::TestEventListeners& listeners =
    ::testing::UnitTest::GetInstance()->listeners();

  listeners.Append(process::ClockTestEventListener::instance());
  listeners.Append(process::FilterTestEventListener::instance());

  return RUN_ALL_TESTS();
}


class BenchmarkProcess : public Process<BenchmarkProcess>
{
public:
  BenchmarkProcess(
      int _iterations = 1,
      int _maxOutstanding = 1,
      const Option<UPID>& _other = Option<UPID>())
    : other(_other),
      counter(0UL),
      done(false),
      iterations(_iterations),
      maxOutstanding(_maxOutstanding),
      outstanding(0),
      sent(0)
  {
    if (other.isSome()) {
      setLink(other.get());
    }
  }

  virtual ~BenchmarkProcess() {}

  virtual void initialize()
  {
    install("ping", &BenchmarkProcess::ping);
    install("pong", &BenchmarkProcess::pong);
  }

  void setLink(const UPID& that)
  {
    link(that);
  }

  void start()
  {
    sendRemaining();
    unique_lock<mutex> lock(_mutex);
    while (!done) {
      condition.wait(lock);
    }
  }

private:
  void ping(const UPID& from, const string& body)
  {
    if (linkedPorts.find(from.port) == linkedPorts.end()) {
      setLink(from);
      linkedPorts.insert(from.port);
    }
    static const string message("hi");
    send(from, "pong", message.c_str(), message.size());
  }

  void pong(const UPID& from, const string& body)
  {
    ++counter;
    --outstanding;
    if (counter >= iterations) {
      lock_guard<mutex> lock(_mutex);
      done = true;
      condition.notify_one();
    }
    sendRemaining();
  }

  void sendRemaining()
  {
    static const string message("hi");
    for (;outstanding < maxOutstanding && sent < iterations;
         ++outstanding, ++sent) {
      send(other.get(), "ping", message.c_str(), message.size());
    }
  }

  Option<UPID> other;

  int counter;

  bool done;
  mutex _mutex;
  condition_variable condition;

  const int iterations;
  const int maxOutstanding;
  int outstanding;
  int sent;
  unordered_set<int> linkedPorts;
};


// A helper function that provides an entry point for a thread. This
// is a back-port of what in c++11 would be a lambda. Launches a
// benchmark process and waits for it to finish.
void benchmarkLauncher(int iterations, int queueDepth, UPID other)
{
  BenchmarkProcess process(iterations, queueDepth, other);
  UPID pid = spawn(&process);
  process.start();
  terminate(process);
  wait(process);
}


// Launch numberOfProcesses processes, each with clients 'client'
// Actors. Play ping pong back and forth between these actors and the
// main 'server' actor. Each 'client' can have queueDepth ping
// requests outstanding to the 'server' actor.
TEST(Process, Process_BENCHMARK_Test)
{
  const int iterations = 2500;
  const int queueDepth = 250;
  const int clients = 8;
  const int numberOfProcesses = 4;

  vector<int> outPipeVector;
  vector<int> inPipeVector;
  vector<pid_t> pidVector;
  for (int moreToLaunch = numberOfProcesses;
       moreToLaunch >= 0; --moreToLaunch) {
    // fork in order to get numberOfProcesses seperate
    // ProcessManagers. This avoids the short-circuit built into
    // ProcessManager for processes communicating in the same manager.
    int pipes[2];
    pid_t pid = -1;
    if(pipe(pipes) < 0) {
      perror("pipe failed");
      abort();
    }
    pid = fork();

    if (pid < 0) {
      perror("fork() failed");
      abort();
    } else if (pid == 0) {
      // Child.

      // Read the number of bytes about to be parsed.
      int32_t stringSize = 0;
      size_t result = read(pipes[0], &stringSize, sizeof(stringSize));
      EXPECT_EQ(result, sizeof(stringSize));
      char buffer[stringSize];
      memset(&buffer, 0, stringSize);

      // Read in the upid of the 'server' actor.
      result = read(pipes[0], &buffer, stringSize);
      EXPECT_EQ(result, stringSize);
      istringstream inStream(buffer);
      UPID other;
      inStream >> other;

      // Launch a thread for each client that backs an actor.
      Stopwatch watch;
      watch.start();
      vector<thread> threadVector;
      for (int i = 0; i < clients; ++i) {
        threadVector.emplace_back(
            benchmarkLauncher,
            iterations,
            queueDepth,
            other);
      }

      // Wait for the clients to finish and join on them.
      foreach (auto& thread, threadVector) {
        thread.join();
      }

      // Compute the total rpcs per second for this process, write the
      // computation back to the server end of the fork.
      double elapsed = watch.elapsed().secs();
      int totalIterations = clients * iterations;
      int rpcPerSecond = totalIterations / elapsed;
      result = write(pipes[1], &rpcPerSecond, sizeof(rpcPerSecond));
      EXPECT_EQ(result, sizeof(rpcPerSecond));
      close(pipes[0]);
      exit(0);
    } else {
      // Parent.

      // Keep track of the pipes to the child forks. This way the
      // results of their rpc / sec computations can be read back and
      // aggregated.
      outPipeVector.emplace_back(pipes[1]);
      inPipeVector.emplace_back(pipes[0]);
      pidVector.emplace_back(pid);

      // If this is the last child launched, then let the parent
      // become the 'server' actor.
      if (moreToLaunch <= 0) {
        BenchmarkProcess process(iterations, queueDepth);
        UPID pid = spawn(&process);

        // Stringify the server pid to send to the child processes.
        ostringstream outStream;
        outStream << pid;
        int32_t stringSize = outStream.str().size();

        // For each child, write the size of the stringified pid as
        // well as the stringified pid to the pipe.
        foreach (int fd, outPipeVector) {
          size_t result = write(fd, &stringSize, sizeof(stringSize));
          EXPECT_EQ(result, sizeof(stringSize));
          result = write(fd, outStream.str().c_str(), stringSize);
          EXPECT_EQ(result, stringSize);
          close(fd);
        }

        // Read the resulting rpcs / second from the child processes
        // and aggregate the results.
        int totalRpcsPerSecond = 0;
        foreach (int fd, inPipeVector) {
          int rpcs = 0;
          size_t result = read(fd, &rpcs, sizeof(rpcs));
          EXPECT_EQ(result, sizeof(rpcs));
          if (result != sizeof(rpcs)) {
            abort();
          }
          totalRpcsPerSecond += rpcs;
        }

        // Wait for all the child forks to terminately gracefully.
        foreach (const auto& p, pidVector) {
          ::waitpid(p, NULL, 0);
        }
        printf("Total: [%ld] rpcs / s\n", totalRpcsPerSecond);
        terminate(process);
        wait(process);
      }
    }
  }
}
