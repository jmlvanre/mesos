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
      size_t _num_iter = 1,
      size_t _max_outstanding = 1,
      const Option<UPID>& _other = Option<UPID>())
      : other(_other),
        counter(0UL),
        done(false),
        num_iter(_num_iter),
        max_outstanding(_max_outstanding),
        outstanding(0),
        sent(0)
  {
    if (other.isSome()) {
      setlink(other.get());
    }
  }

  virtual ~BenchmarkProcess() {
  }

  virtual void initialize()
  {
    install("ping", &BenchmarkProcess::ping);
    install("pong", &BenchmarkProcess::pong);
  }

  void setlink(const UPID& that) {
    link(that);
  }

  void start() {
    const char* msg = "hi";
    for (;outstanding < max_outstanding &&
        sent < num_iter; ++outstanding, ++sent) {
      send(other.get(), "ping", msg, strlen(msg));
    }
    unique_lock<mutex> lock(mut);
    while (!done) {
      cond.wait(lock);
    }
  }

private:
  void ping(const UPID& from, const string& body) {
    if (linked_ports.find(from.port) == linked_ports.end()) {
      setlink(from);
      linked_ports.emplace(from.port);
    }
    const char* msg = "hi";
    send(from, "pong", msg, strlen(msg));
  }

  void pong(const UPID& from, const string& body) {
    ++counter;
    --outstanding;
    if (counter >= num_iter) {
      lock_guard<mutex> lock(mut);
      done = true;
      cond.notify_one();
    }
    const char* msg = "hi";
    for (;outstanding < max_outstanding &&
        sent < num_iter; ++outstanding, ++sent) {
      send(other.get(), "ping", msg, strlen(msg));
    }
  }

  Option<UPID> other;

  size_t counter;

  bool done;
  mutex mut;
  condition_variable cond;

  const size_t num_iter;
  const size_t max_outstanding;
  size_t outstanding;
  size_t sent;
  unordered_set<int> linked_ports;
};


TEST(Process, Process_BENCHMARK_Test)
{
  const size_t num_iter = 2500;
  const size_t queue_depth = 250;
  const size_t num_threads = 8;
  const size_t num_proc = 4;

  vector<int> out_pipe_vec;
  vector<int> in_pipe_vec;
  vector<pid_t> pid_vec;
  function<void (size_t)> do_fork = [&](size_t more_to_launch) {
    // fork in order to get num_proc seperate ProcessManagers. This
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
      auto launcher = [&]() {
        BenchmarkProcess process(num_iter, queue_depth, other);
        UPID pid = spawn(&process);
        process.start();
        terminate(process);
        wait(process);
      };
      Stopwatch watch;
      watch.start();
      vector<thread> tvec;
      for (size_t i = 0; i < num_threads; ++i) {
        tvec.emplace_back(launcher);
      }
      for (auto &t : tvec) {
        t.join();
      }
      double elapsed = watch.elapsed().secs();
      size_t total_iter = num_threads * num_iter;
      size_t rpc_per_sec = total_iter / elapsed;
      r = write(pipes[1], &rpc_per_sec, sizeof(rpc_per_sec));
      EXPECT_EQ(r, sizeof(rpc_per_sec));
      close(pipes[0]);
      exit(0);
    } else {
      // parent
      out_pipe_vec.emplace_back(pipes[1]);
      in_pipe_vec.emplace_back(pipes[0]);
      pid_vec.emplace_back(pid);
      if (more_to_launch <= 1) {
        BenchmarkProcess process(num_iter, queue_depth);
        UPID pid = spawn(&process);
        ostringstream ss;
        ss << pid;
        int32_t strsize = ss.str().size();
        for (auto fd : out_pipe_vec) {
          size_t w = write(fd, &strsize, sizeof(strsize));
          EXPECT_EQ(w, sizeof(strsize));
          w = write(fd, ss.str().c_str(), strsize);
          EXPECT_EQ(w, strsize);
          close(fd);
        }
        size_t total_rpcs_per_sec = 0;
        for (auto fd : in_pipe_vec) {
          size_t rpcs = 0;
          size_t r = read(fd, &rpcs, sizeof(rpcs));
          EXPECT_EQ(r, sizeof(rpcs));
          if (r != sizeof(rpcs)) {
            abort();
          }
          total_rpcs_per_sec += rpcs;
        }
        for (const auto &p : pid_vec) {
          ::waitpid(p, nullptr, 0);
        }
        printf("Total: [%ld] rpcs / s\n", total_rpcs_per_sec);
        terminate(process);
        wait(process);
      } else {
        do_fork(more_to_launch - 1);
      }
    }
  };
  do_fork(num_proc);
}
