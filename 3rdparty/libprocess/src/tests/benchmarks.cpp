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
#include <boost/concept_check.hpp>

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
      size_t _numIter = 1,
      size_t _maxOutstanding = 1,
      const Option<UPID>& _other = Option<UPID>())
      : other(_other),
        counter(0UL),
        done(false),
        numIter(_numIter),
        maxOutstanding(_maxOutstanding),
        outstanding(0),
        sent(0)
  {
    if (other.isSome()) {
      setlink(other.get());
    }
  }

  virtual ~BenchmarkProcess() {}

  virtual void initialize()
  {
    install("ping", &BenchmarkProcess::ping);
    install("pong", &BenchmarkProcess::pong);
  }

  void setlink(const UPID& that) {
    link(that);
  }

  void start() {
    sendRemaining();
    unique_lock<mutex> lock(mut);
    while (!done) {
      cond.wait(lock);
    }
  }

private:
  void ping(const UPID& from, const string& body)
  {
    if (linkedPorts.find(from.port) == linkedPorts.end()) {
      setlink(from);
      linkedPorts.emplace(from.port);
    }
    static const std::string message("hi");
    send(from, "pong", message.c_str(), message.size());
  }

  void pong(const UPID& from, const string& body)
  {
    ++counter;
    --outstanding;
    if (counter >= numIter) {
      lock_guard<mutex> lock(mut);
      done = true;
      cond.notify_one();
    }
    sendRemaining();
  }

  void sendRemaining()
  {
    static const std::string message("hi");
    for (;outstanding < maxOutstanding && sent < numIter;
         ++outstanding, ++sent) {
      send(other.get(), "ping", message.c_str(), message.size());
    }
  }

  Option<UPID> other;

  size_t counter;

  bool done;
  mutex mut;
  condition_variable cond;

  const size_t numIter;
  const size_t maxOutstanding;
  size_t outstanding;
  size_t sent;
  unordered_set<int> linkedPorts;
};


void benchmarkLauncher(size_t numIter, size_t queueDepth, UPID other)
{
  BenchmarkProcess process(numIter, queueDepth, other);
  UPID pid = spawn(&process);
  process.start();
  terminate(process);
  wait(process);
}


/* Launch numProcs processes, each with numClients 'client' Actors.
 * Play ping pong back and forth between these actors and the main
 * 'server' actor. Each 'client' can have queueDepth ping requests
 * outstanding to the 'server' actor. */
TEST(Process, Process_BENCHMARK_Test)
{
  const size_t numIter = 2500;
  const size_t queueDepth = 250;
  const size_t numClients = 8;
  const size_t numProcs = 4;

  vector<int> outPipeVec;
  vector<int> inPipeVec;
  vector<pid_t> pidVec;
  for (int64_t moreToLaunch = numProcs; moreToLaunch >= 0; --moreToLaunch) {
    // fork in order to get numProcs seperate ProcessManagers. This
    // avoids the short-circuit built into ProcessManager for
    // processes communicating in the same manager.
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
      // child
      int32_t strsize = 0;
      size_t r = read(pipes[0], &strsize, sizeof(strsize));
      EXPECT_EQ(r, sizeof(strsize));
      char buf[strsize];
      memset(&buf, 0, strsize);
      r = read(pipes[0], &buf, strsize);
      EXPECT_EQ(r, strsize);
      istringstream iss(buf);
      UPID other;
      iss >> other;
      Stopwatch watch;
      watch.start();
      vector<thread> tvec;
      for (size_t i = 0; i < numClients; ++i) {
        tvec.emplace_back(benchmarkLauncher, numIter, queueDepth, other);
      }
      foreach (auto& t, tvec) {
        t.join();
      }
      double elapsed = watch.elapsed().secs();
      size_t totalIter = numClients * numIter;
      size_t rpcPerSec = totalIter / elapsed;
      r = write(pipes[1], &rpcPerSec, sizeof(rpcPerSec));
      EXPECT_EQ(r, sizeof(rpcPerSec));
      close(pipes[0]);
      exit(0);
    } else {
      // parent
      outPipeVec.emplace_back(pipes[1]);
      inPipeVec.emplace_back(pipes[0]);
      pidVec.emplace_back(pid);
      if (moreToLaunch <= 0) {
        BenchmarkProcess process(numIter, queueDepth);
        UPID pid = spawn(&process);
        ostringstream ss;
        ss << pid;
        int32_t strsize = ss.str().size();
        foreach (int fd, outPipeVec) {
          size_t w = write(fd, &strsize, sizeof(strsize));
          EXPECT_EQ(w, sizeof(strsize));
          w = write(fd, ss.str().c_str(), strsize);
          EXPECT_EQ(w, strsize);
          close(fd);
        }
        size_t totalRpcsPerSec = 0;
        foreach (int fd, inPipeVec) {
          size_t rpcs = 0;
          size_t r = read(fd, &rpcs, sizeof(rpcs));
          EXPECT_EQ(r, sizeof(rpcs));
          if (r != sizeof(rpcs)) {
            abort();
          }
          totalRpcsPerSec += rpcs;
        }
        foreach (const auto& p, pidVec) {
          ::waitpid(p, nullptr, 0);
        }
        printf("Total: [%ld] rpcs / s\n", totalRpcsPerSec);
        terminate(process);
        wait(process);
      }
    }
  }
}
