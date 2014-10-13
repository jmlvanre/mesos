#ifndef __PROCESS_GC_HPP__
#define __PROCESS_GC_HPP__

#include <map>

#include <process/process.hpp>


namespace process {

class GarbageCollector : public Process<GarbageCollector>
{
public:
  GarbageCollector() : ProcessBase("__gc__") {}
  virtual ~GarbageCollector() {}

  template <typename T>
  void manage(const T* t)
  {
    const ProcessBase* process = t;
    if (process != NULL) {
      processes[process->self()] = std::unique_ptr<const ProcessBase>(process);
      link(process->self());
    }
  }

protected:
  virtual void exited(const UPID& pid)
  {
    const auto& iter = processes.find(pid);
    if (iter != processes.end()) {
      processes.erase(iter);
    }
  }

  virtual void finalize()
  {
    foreachpair(
        const UPID& pid,
        std::unique_ptr<const ProcessBase>& process,
        processes) {
      terminate(pid);
      wait(pid);
      process.reset();
    }
    processes.clear();
  }

private:
  std::map<UPID, std::unique_ptr<const ProcessBase>> processes;
};


extern PID<GarbageCollector> gc;

} // namespace process {

#endif // __PROCESS_GC_HPP__
