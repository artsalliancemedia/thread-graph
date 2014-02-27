"""
(c) 2014 Arts Alliance Media

MultiThreaded profiler aggregating data from other profiler libraries.
"""

from os import getpid
from os import makedirs
from os import path
import sys
from time import time
import threading
import traceback

#from pympler import muppy
from pympler.process import ProcessMemoryInfo


def getProcessMemory():
    """Utility function that defind the logic to get memory."""
    return ProcessMemoryInfo().rss
    #return muppy.get_size(muppy.get_objects())


class ThreadLocals(object):
    """Storage class for thread-local infirmation.

    Decorates threading.local with freandlier getters.
    """
    def __init__(self, *args, **kwargs):
        super(ThreadLocals, self).__init__(*args, **kwargs)
        self.locals_ = threading.local()

    def initialize(self):
        """Initialization needs to be done once per-thread.

        This also needs to be done in the thread requestig data and not
        in the thread creating the object.
        To solve the problem each getter attemps to initialize the
        local variables and does it only once.
        """
        try:
            return self.locals_.initialized
        except AttributeError:
            self.locals_.initialized = True
            self.locals_.thread_name = threading.current_thread().getName()
            return True

    def getThreadName(self):
        self.initialize()
        return self.locals_.thread_name


class ProcessProfile(object):
    """Keeps track of profiling information for the current process.

    Profiling information is processed and stored per-thread to reduce
    shared resources and this class serves as a registry for that information.

    Another interesting tool is https://github.com/wyplay/pytracemalloc
    but it requires a patched CPython implementation.

    Thread safety:
      Please not that this implementation uses a CPython implementation detail
      to guarantee atomic maipulation of shared resources.
      This detail is called "the global interpreter lock" and is explained in the
      python documentation: http://docs.python.org/3.4/glossary.html#term-global-interpreter-lock

      To save some time let me confirm your fear: the cpython implementation
      prevents multiple threads from running python code at the same time.
      You are still good when doing I/O operations, but execution of python
      bytecode is limited to one thread at a time.
    """
    def __init__(self, stream_factory=None, default_log_path=None,
                 profile=None):
        """Creates a new process profiler.

        Args:
            stream_factory: A function that returns a per-thread file object
                            to which information is written.
            default_log_path: Prefix added by the default stream factory when
                              creating log files.
            profile: type of events to profile, one of "c", "python" or "both".
        """
        super(ProcessProfile, self).__init__()
        self.stream_factory_ = (stream_factory if stream_factory else
                                self.default_stream_factory)
        self.default_log_path_ = default_log_path if default_log_path else "."
        self.profile_ = profile
        self.threads_ = {}
        self.previous_profiler_ = None
        self.main_pid_ = getpid()
        self.locals_ = ThreadLocals()  # Per-thread locals.

        # Store process-level memory.
        self.openProcessStream()

        # Store process-level tweeks.
        self.proc_mem_check_ = 0
        self.proc_mem_freq_ = 1000
        self.profile_forked_ = False

        # Store defaults for new thread profilers.
        self.filter_ = ""
        self.mem_ = True
        self.times_ = True
        self.sleep_ = True
        self.stack_ = False

    def default_stream_factory(self, stream_type):
        """Creates a file for each thread to log data to.

        This is the default function used by the profiler.

        Args:
          stream_type: Type of information to be stored in the stream.
        """
        filename = "{0}.{1}".format(
            self.locals_.getThreadName(), stream_type)
        basepath = path.join(self.default_log_path_, str(getpid()))
        filename = path.join(basepath, filename)
        if not path.exists(basepath):
            makedirs(basepath)
        return open(filename, "w")

    def disable(self):
        """Stop profiling the program and restore previous profile function."""
        sys.setprofile(self.previous_profiler_)
        threading.setprofile(self.previous_profiler_)

    def disableForkedProfile(self):
        """Do not profile processes forked off the current one."""
        self.profile_forked_ = False

    def dispatch(self, frame, event, arg):
        """Dispatches the event to the appropaite thread profiler."""
        try:
            # When subprocesses are created they will share the profile
            # function and open file descriptors.
            # Check here if the PID changed and react appropriately.
            if getpid() != self.main_pid_:
                if self.profile_forked_:
                    self.proc_mem_.close()
                    self.openProcessStream()
                    self.threads_ = {}
                    self.main_pid_ = getpid()
                else:
                    self.disable()
                    return None

            # Log process-level memory usage.
            if self.proc_mem_freq_:
                if self.proc_mem_check_ == 0:
                    stamp = str(time()) + "#" if self.times_ else ""
                    self.proc_mem_.write("{0}{1}\n".format(
                        stamp, getProcessMemory()))
                    self.proc_mem_.flush()
                self.proc_mem_check_ = ((self.proc_mem_check_ + 1) %
                                        self.proc_mem_freq_)

            # Dispatch to thread-level profiler.
            thread = self.locals_.getThreadName()
            thread_stats = self.threads_.get(thread)
            if thread_stats is None:
                thread_stats = ThreadProfile(
                    stream_factory=self.stream_factory_, profile=self.profile_,
                    track_memory=self.mem_, track_times=self.times_,
                    track_stack=self.stack_, track_sleep=self.sleep_)
                thread_stats.setFilter(self.filter_)
                self.threads_[thread] = thread_stats
            thread_stats.dispatch(frame, event, arg)
            return self.dispatch
        # Used to debug.
        except Exception as e:
            traceback.print_exc()
            raise
        except:
            # Python does not allow exceptions to be raised by the profile
            # function and silently terminates the process if that happens.
            # Catch all to prevent unexpected and unexplained terminations.
            return self.dispatch

    def enable(self):
        """Start profiling the program."""
        self.previous_profiler_ = sys.getprofile()
        threading.setprofile(self.dispatch)
        sys.setprofile(self.dispatch)

    def enableForkedProfile(self):
        """Profile processes forked off the current one."""
        self.profile_forked_ = True

    def logTimestamps(self, enable=True):
        """Enables or disables collection timestamps."""
        self.times_ = enable
        for thread in self.threads_.values():
            thread.logTimestamps(enable)

    def openProcessStream(self):
        basepath = path.join(self.default_log_path_, str(getpid()))
        filename = path.join(basepath, "process.mem")
        if not path.exists(basepath):
            makedirs(basepath)
        self.proc_mem_ = open(filename, "w")

    def setFilter(self, filter):
        """Sets a file filter for new and running threads.

        When a function call is intercepted the filename in which it is defined
        is checked against the filter. If the filter is a prifix of the
        function filename than the function is profiled, otherwise it is
        ignored. If no filter is defined all functions are profiled.
        """
        self.filter_ = filter
        for thread in self.threads_.values():
            thread.setFilter(filter)

    def setProcessMemoryFrequence(self, ferq):
        """Sets process-level memory collection frequency.

        Process-level memory is collected at any event with a sampling
        approach to avoid performance impact and log flooding.
        The frequency defines how many events will be ignored between
        collections.

        Set this value to None to disable process-level memory collection.
        """
        self.proc_mem_freq_ = freq

    def trackMemory(self, enable=True):
        """Enables or disables memory traking for new and running threads."""
        self.mem_ = enable
        for thread in self.threads_.values():
            thread.trackMemory(enable)

    def trackSleeps(self, enable=True):
        """Enables or disables sleep traking for new and running threads.

        When sleep traking is enabled the thread profiler will keep track of
        memory allocated during a call to sleep and assumes that the memory
        was not allocated by this thread (since it was sleeping).

        When the profiler requests the current memory allocation, the
        ammount of memory allocated during sleeps will be deducted.
        This provides an estimate of memory consumption at a thread level.
        Since the approach is not (and cannot be) accurate it can lead to
        strange and/or inconsistent measurments.
        If that happends you are encuraged to disable this feature.
        """
        self.mem_ = enable
        for thread in self.threads_.values():
            thread.trackMemory(enable)

    def trackStack(self, enable=True):
        """Enables or disables stack traking for new and running threads."""
        self.stack_ = enable
        for thread in self.threads_.values():
            thread.trackStack(enable)


class ThreadProfile(object):
    """Profiles a single thread.

    Events are received from a ProcessProfile through the dispatch method
    and are assumed to be for the correct thread.

    For more information on profiling see:
        * Python profile module for an example
            http://hg.python.org/cpython/file/default/Lib/profile.py
        * Docs on the arguments: http://docs.python.org/3.4/library/inspect.html
        * sys.settrace: http://docs.python.org/3.4/library/sys.html#sys.settrace
    """
    def __init__(self, stream_factory=None, profile=None, track_memory=True,
                 track_times=True, track_stack=False, track_sleep=True):
        """Creates a per-thread profiler.

        Args:
            stream_factory: callable that returns a writable file.
            profile: choose what to profile, "c", "python" or "both".
        """
        dispatchers = {
            "c": {
                "c_call": self.handleCIn_,
                "c_exception": self.handleCOut_,
                "c_return": self.handleCOut_
            },
            "python": {
                "call": self.handleIn_,
                "exception": self.handleOut_,
                "return": self.handleOut_
            },
            "both": {
                "call": self.handleIn_,
                "exception": self.handleOut_,
                "return": self.handleOut_,
                "c_call": self.handleCIn_,
                "c_exception": self.handleCOut_,
                "c_return": self.handleCOut_
            }
        }
        self.dispatcher_ = dispatchers[profile if profile else "both"]

        # Per-thread information used during collection.
        self.frames_ = {}
        self.sleep_accounting_ = 0
        self.sleep_frames_ = {}
        self.stack_level_ = 0

        # Data collection tweeks.
        self.file_filter_ = ""
        self.mem_ = track_memory
        self.times_ = track_times
        self.stack_ = track_stack
        self.sleep_trak_ = track_sleep

        # Create required streams.
        self.mem_stream_ = stream_factory("mem") if self.mem_ else None
        self.stack_stream_ = stream_factory("stack") if self.stack_ else None

    def closeStreams(self):
        try:
            if self.mem_stream_:
                self.mem_stream_.close()
            if self.stack_stream_:
                self.stack_stream_.close()
        except IOError:
            pass

    def dispatch(self, frame, event, arg):
        """Entry point for event dispatch.

        Args:
            frame: the frame executing at the time of interrupt.
            event: the event that triggered the interrupt.
            arg:   the arguments associated with the event.
        """
        if self.sleep_trak_ and event == "c_call" and arg.__name__ == "sleep":
            self.sleep_frames_[id(frame)] = self.getMemory_()
        elif (self.sleep_trak_ and event == "c_return" and
              arg.__name__ == "sleep" and id(frame) in self.sleep_frames_):
            mem_before = self.sleep_frames_[id(frame)];
            del self.sleep_frames_[id(frame)]
            mem_after = self.getMemory_()
            self.sleep_accounting_ += mem_after - mem_before
        if event in self.dispatcher_:
            self.dispatcher_[event](frame, event, arg)

    def getMemory_(self):
        """Internally used to fetch memory."""
        return getProcessMemory() - self.sleep_accounting_

    def handleIn_(self, frame, event, arg, name=None):
        """Handles a function call.
        Args:
            frame: The frame object passed by CPython.
            event: The event that triggered the profile function.
            arg: Additional argument passed by CPython, depends on event.
            name: If specified, it is the name of the function being called.
        """
        if not frame.f_code.co_filename.startswith(self.file_filter_): return
        if self.stack_:
            now = str(time()) + "#" if self.times_ else ""
            self.stack_stream_.write("{0}{1}{2}:{3}:{4}\n".format(
                " ".join([""] * self.stack_level_), now,
                frame.f_code.co_filename, frame.f_lineno,
                name if name else frame.f_code.co_name))
            self.stack_stream_.flush()
        self.stack_level_ += 1
        self.frames_[id(frame)] = (
            self.getMemory_() if self.mem_ else 0,
            name if name else frame.f_code.co_name)

    def handleOut_(self, frame, event, arg):
        """Handles a function return (even in case of exception).
        Args:
            frame: The frame object passed by CPython.
            event: The event that triggered the profile function.
            arg: Additional argument passed by CPython, depends on event.
        """
        if id(frame) in self.frames_:
            self.stack_level_ -= 1
            (mem_before, name) = self.frames_[id(frame)]
            del self.frames_[id(frame)]
            if self.mem_:
                mem_after = self.getMemory_()
                mem_delta = mem_after - mem_before
                now = str(time()) + "#" if self.times_ else ""
                self.mem_stream_.write("{0}{1}:{2}:{3}=>{4}\n".format(
                    now, frame.f_code.co_filename, frame.f_lineno, name,
                    mem_delta))
                self.mem_stream_.flush()

    def handleCIn_(self, frame, event, arg):
        """Handles a C function call.
        Args:
            frame: The frame object passed by CPython.
            event: The event that triggered the profile function.
            arg: Additional argument passed by CPython, depends on event.
        """
        self.handleIn_(frame, event, arg, arg.__name__)

    def handleCOut_(self, frame, event, arg):
        """Handles a C function return (even in case of exception).
        Args:
            frame: The frame object passed by CPython.
            event: The event that triggered the profile function.
            arg: Additional argument passed by CPython, depends on event.
        """
        self.handleOut_(frame, event, arg)

    def logTimestamps(self, enable=True):
        """Enables or disables collection timestamps."""
        self.times_ = enable

    def setFilter(self, filter):
        """Sets the file filter for the thread."""
        self.file_filter_ = filter

    def trackMemory(self, enable=True):
        """Enable or disable memory tracking."""
        self.mem_ = enable

    def trackStack(self, enable=True):
        """Enables or disables stack traking."""
        self.stack_ = enable
