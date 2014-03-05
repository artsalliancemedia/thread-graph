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
#import traceback

from pympler.process import ProcessMemoryInfo


def _getProcessMemory():
    """Utility function that defined the logic to get memory."""
    return ProcessMemoryInfo().rss


class _ThreadLocals(object):
    """Storage class for thread-local information.

    Decorates threading.local with friendlier getters.
    """
    def __init__(self, *args, **kwargs):
        super(_ThreadLocals, self).__init__(*args, **kwargs)
        self._locals = threading.local()

    def _initialize(self):
        """Initialization needs to be done once per-thread.

        This also needs to be done in the thread requesting data and not
        in the thread creating the object.
        To solve the problem each getter attempts to initialize the
        local variables and does it only once.
        """
        try:
            return self._locals.initialized
        except AttributeError:
            self._locals.initialized = True
            self._locals.thread_name = threading.current_thread().getName()
            return True

    def getThreadName(self):
        self._initialize()
        return self._locals.thread_name


class ProcessProfile(object):
    """Keeps track of profiling information for the current process.

    Profiling information is processed and stored per-thread to reduce
    shared resources and this class serves as a registry for that information.

    Another interesting tool is https://github.com/wyplay/pytracemalloc
    but it requires a patched CPython implementation.

    Thread safety:
      Please not that this implementation uses a CPython implementation detail
      to guarantee atomic manipulation of shared resources.
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
        self._stream_factory = (stream_factory if stream_factory else
                                self.default_stream_factory)
        self._default_log_path = default_log_path if default_log_path else "."
        self._profile = profile
        self._threads = {}
        self._previous_profiler = None
        self._main_pid = getpid()
        self._locals = _ThreadLocals()  # Per-thread locals.

        # Store process-level memory.
        self._openProcessStream()

        # Store process-level tweeks.
        self._proc_mem_check = 0
        self._proc_mem_freq = 1000
        self._profile_forked = False

        # Store defaults for new thread profilers.
        self._filter = ""
        self._mem = True
        self._times = True
        self._sleep = True
        self._stack = False

    def _dispatch(self, frame, event, arg):
        """Dispatches the event to the appropriate thread profiler."""
        try:
            # It seems that sometimes, when the VM exits and the profiler
            # is enabled the dispatch method is called during the tear-down
            # as well.
            # We use getpid is None as a sign of tear-down and bail.
            if getpid is None:
                self.disable()
                return None

            # When subprocesses are created they will share the profile
            # function and open file descriptors.
            # Check here if the PID changed and react appropriately.
            if getpid() != self._main_pid:
                if self._profile_forked:
                    self._proc_mem.close()
                    self._openProcessStream()
                    self._threads = {}
                    self._main_pid = getpid()
                else:
                    self.disable()
                    return None

            # Log process-level memory usage.
            if self._proc_mem_freq:
                if self._proc_mem_check == 0:
                    stamp = str(time()) + "#" if self._times else ""
                    self._proc_mem.write("{0}{1}\n".format(
                        stamp, _getProcessMemory()))
                    self._proc_mem.flush()
                self._proc_mem_check = ((self._proc_mem_check + 1) %
                                        self._proc_mem_freq)

            # Dispatch to thread-level profiler.
            thread = self._locals.getThreadName()
            thread_stats = self._threads.get(thread)
            if thread_stats is None:
                thread_stats = ThreadProfile(
                    stream_factory=self._stream_factory, profile=self._profile,
                    track_memory=self._mem, track_times=self._times,
                    track_stack=self._stack, track_sleep=self._sleep)
                thread_stats.setFilter(self._filter)
                self._threads[thread] = thread_stats
            thread_stats._dispatch(frame, event, arg)
            return self._dispatch
        # Used to debug tool.
        #except Exception as e:
        #    print(e)
        #    print(traceback.format_exc())
        #    raise
        except:
            # Python does not allow exceptions to be raised by the profile
            # function and silently terminates the process if that happens.
            # Catch all to prevent unexpected and unexplained terminations.
            return self._dispatch

    def _openProcessStream(self):
        basepath = path.join(self._default_log_path, str(getpid()))
        filename = path.join(basepath, "process.mem")
        if not path.exists(basepath):
            makedirs(basepath)
        self._proc_mem = open(filename, "w")

    def default_stream_factory(self, stream_type):
        """Creates a file for each thread to log data to.

        This is the default function used by the profiler.

        Args:
          stream_type: Type of information to be stored in the stream.
        """
        filename = "{0}.{1}".format(
            self._locals.getThreadName(), stream_type)
        basepath = path.join(self._default_log_path, str(getpid()))
        filename = path.join(basepath, filename)
        if not path.exists(basepath):
            makedirs(basepath)
        return open(filename, "w")

    def disable(self):
        """Stop profiling the program and restore previous profile function."""
        sys.setprofile(self._previous_profiler)
        threading.setprofile(self._previous_profiler)

    def disableForkedProfile(self):
        """Do not profile processes forked off the current one."""
        self._profile_forked = False

    def enable(self):
        """Start profiling the program."""
        self._previous_profiler = sys.getprofile()
        threading.setprofile(self._dispatch)
        sys.setprofile(self._dispatch)

    def enableForkedProfile(self):
        """Profile processes forked off the current one."""
        self._profile_forked = True

    def logTimestamps(self, enable=True):
        """Enables or disables collection timestamps."""
        self._times = enable
        for thread in self._threads.values():
            thread.logTimestamps(enable)

    def setFilter(self, filter):
        """Sets a file filter for new and running threads.

        When a function call is intercepted the filename in which it is defined
        is checked against the filter. If the filter is a prefix of the
        function filename than the function is profiled, otherwise it is
        ignored. If no filter is defined all functions are profiled.
        """
        self._filter = filter
        for thread in self._threads.values():
            thread.setFilter(filter)

    def setProcessMemoryFrequence(self, freq):
        """Sets process-level memory collection frequency.

        Process-level memory is collected at any event with a sampling
        approach to avoid performance impact and log flooding.
        The frequency defines how many events will be ignored between
        collections.

        Set this value to None to disable process-level memory collection.
        """
        self._proc_mem_freq = freq

    def trackMemory(self, enable=True):
        """Enables or disables memory tracking for new and running threads."""
        self._mem = enable
        for thread in self._threads.values():
            thread.trackMemory(enable)

    def trackSleeps(self, enable=True):
        """Enables or disables sleep tracking for new and running threads.

        When sleep tracking is enabled the thread profiler will keep track of
        memory allocated during a call to sleep and assumes that the memory
        was not allocated by this thread (since it was sleeping).

        When the profiler requests the current memory allocation, the
        amount of memory allocated during sleeps will be deducted.
        This provides an estimate of memory consumption at a thread level.
        Since the approach is not (and cannot be) accurate it can lead to
        strange and/or inconsistent measurements.
        If that happens you are encouraged to disable this feature.
        """
        self._mem = enable
        for thread in self._threads.values():
            thread.trackMemory(enable)

    def trackStack(self, enable=True):
        """Enables or disables stack tracking for new and running threads."""
        self._stack = enable
        for thread in self._threads.values():
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
            stream_factory: callable that returns a writeable file.
            profile: choose what to profile, "c", "python" or "both".
        """
        dispatchers = {
            "c": {
                "c_call": self._handleCIn,
                "c_exception": self._handleCOut,
                "c_return": self._handleCOut
            },
            "python": {
                "call": self._handleIn,
                "exception": self._handleOut,
                "return": self._handleOut
            },
            "both": {
                "call": self._handleIn,
                "exception": self._handleOut,
                "return": self._handleOut,
                "c_call": self._handleCIn,
                "c_exception": self._handleCOut,
                "c_return": self._handleCOut
            }
        }
        self._dispatcher = dispatchers[profile if profile else "both"]

        # Per-thread information used during collection.
        self._frames = {}
        self._sleep_accounting = 0
        self._sleep_frames = {}
        self._stack_level = 0

        # Data collection tweeks.
        self._file_filter = ""
        self._mem = track_memory
        self._times = track_times
        self._stack = track_stack

        # Attempt to recognize context switch returns
        # The idea is simple: if the return is from a function that is
        # likely to trigger a context switch the memory delta registered
        # at the return event is ignored from the thread memory usage.
        self._sleep_trak = track_sleep
        self._sleep_triggers = {
            "time": set(["sleep"]),
            "/usr/lib/python2.6/threading.py": set(["acquire"]),
            None: set(["acquire"])  # For some reason acquire seems to belong to the None module.
        }

        # Create required streams.
        self._mem_stream = stream_factory("mem") if self._mem else None
        self._stack_stream = stream_factory("stack") if self._stack else None

    def _dispatch(self, frame, event, arg):
        """Entry point for event dispatch.

        Args:
            frame: the frame executing at the time of interrupt.
            event: the event that triggered the interrupt.
            arg:   the arguments associated with the event.
        """
        if (self._sleep_trak and event == "c_call" and
            arg.__module__ in self._sleep_triggers and
            arg.__name__ in self._sleep_triggers[arg.__module__]):
            self._sleep_frames[id(frame)] = self._getMemory()
        elif (self._sleep_trak and (event == "c_return" or
              event == "c_exception") and id(frame) in self._sleep_frames):
            mem_before = self._sleep_frames[id(frame)];
            del self._sleep_frames[id(frame)]
            mem_after = self._getMemory()
            self._sleep_accounting += mem_after - mem_before

        if event in self._dispatcher:
            self._dispatcher[event](frame, event, arg)

    def _getMemory(self):
        """Internally used to fetch memory."""
        return _getProcessMemory() - self._sleep_accounting

    def _handleIn(self, frame, event, arg, fid=None, name=None, filename=None):
        """Handles a function call.
        Args:
            frame: The frame object passed by CPython.
            event: The event that triggered the profile function.
            arg: Additional argument passed by CPython, depends on event.
            fid: If specified, it is the id of the frame.
            name: If specified, it is the name of the function being called.
            filename: If specified, it is the filename containing the function being called.
        """
        filename = filename if filename else frame.f_code.co_filename
        name = name if name else frame.f_code.co_name
        fid = fid if fid else id(frame)
        if not filename.startswith(self._file_filter): return
        if self._stack:
            now = str(time()) + "#" if self._times else ""
            self._stack_stream.write("{0}{1}{2}:{3}:{4}\n".format(
                " ".join([""] * self._stack_level), now, filename,
                frame.f_lineno, name))
            self._stack_stream.flush()
        self._stack_level += 1
        self._frames[fid] = (
            self._getMemory() if self._mem else 0, name, filename)

    def _handleOut(self, frame, event, arg, fid=None):
        """Handles a function return (even in case of exception).
        Args:
            frame: The frame object passed by CPython.
            event: The event that triggered the profile function.
            arg: Additional argument passed by CPython, depends on event.
            fid: If specified, it is the id of the frame.
        """
        fid = fid if fid else id(frame)
        if fid in self._frames:
            self._stack_level -= 1
            (mem_before, name, filename) = self._frames[fid]
            del self._frames[fid]
            if self._mem:
                mem_after = self._getMemory()
                mem_delta = mem_after - mem_before
                now = str(time()) + "#" if self._times else ""
                self._mem_stream.write("{0}{1}:{2}:{3}=>{4}\n".format(
                    now, filename, frame.f_lineno, name, mem_delta))
                self._mem_stream.flush()

    def _handleCIn(self, frame, event, arg):
        """Handles a C function call.
        Args:
            frame: The frame object passed by CPython.
            event: The event that triggered the profile function.
            arg: Additional argument passed by CPython, depends on event.
        """
        self._handleIn(frame, event, arg, -id(frame), arg.__name__,
                       arg.__module__)

    def _handleCOut(self, frame, event, arg):
        """Handles a C function return (even in case of exception).
        Args:
            frame: The frame object passed by CPython.
            event: The event that triggered the profile function.
            arg: Additional argument passed by CPython, depends on event.
        """
        self._handleOut(frame, event, arg, -id(frame))

    def closeStreams(self):
        try:
            if self._mem_stream:
                self._mem_stream.close()
            if self._stack_stream:
                self._stack_stream.close()
        except IOError:
            pass

    def logTimestamps(self, enable=True):
        """Enables or disables collection timestamps."""
        self._times = enable

    def setFilter(self, filter):
        """Sets the file filter for the thread."""
        self._file_filter = filter

    def trackMemory(self, enable=True):
        """Enable or disable memory tracking."""
        self._mem = enable

    def trackStack(self, enable=True):
        """Enables or disables stack tracking."""
        self._stack = enable
