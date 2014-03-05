"""
(c) 2014 Arts Alliance Media

Post-process Profiler dumps and generate graphs.
"""


from __future__ import print_function

import argparse
from datetime import datetime
import math
import os
import subprocess
import sys
import tempfile
from time import mktime

import StackTree
from StackTree import count_spaces


class _Markers(object):
    """Tracks strings and returns shorter markers."""
    def __init__(self):
        self._mark = 0
        self._map = {}

    def _getMark(self):
        return str(self._mark)

    def _nextMark(self):
        self._mark += 1

    def newMark(self, element, profile=None):
        profile = profile + ">" if profile else ""
        mark    = self._getMark()
        self._map[mark] = profile + element
        self._nextMark()
        return mark

    def getElement(self, mark):
        return self._map[mark]

    def iter(self):
        return self._map.keys()


# Define file parsers.
def _parse_datetime(string):
    """Convert a user friendly time string into a UNIX timestamps."""
    if string is None:
        return ""
    with_date = ["%d/%m/%Y %H:%M", "%d/%m/%y %H:%M", "%d/%m/%Y", "%d/%m/%y"]
    without_date = ["%H:%M"]
    time = None
    for format in with_date:
        try:
            time = datetime.strptime(string, format)
            break
        except ValueError:
            pass
    for format in without_date:
        try:
            time = datetime.strptime(string, format)
            now = datetime.now()
            time = time.replace(now.year, now.month, now.day)
            break
        except ValueError:
            pass
    if time:
        return '"{0}"'.format(mktime(time.timetuple()))
    raise ValueError()


def _parse_process_memory(line):
    (time, mem) = line.split("#")
    return (time, int(mem))


def _parse_thread_memory(line, timed):
    if timed:
        (time, line) = line.split("#")
        time = float(time)
    else:
        time = None
    (name, mem) = line.split("=>")
    return (time, name, int(mem))


def _parse_thread_stack(line, timed):
    (level, line) = count_spaces(line)
    if timed:
        (time, line) = line.split("#")
        time = float(time)
    else:
        time = None
    return (level, time, line)


# Processing functions.
def memg(args):
    """Graphs the memory usage of each thread and the process.

    Converts the given files into a gnuplot compatible format
    then calls gnuplot to create the graph.
    Also creates a legend file that names the peaks marked in the graph.

    The files must meet the following assumptions:
      * Each line in non-empty files has the form TIME#NAME=>MEM
          where TIME# is a Unix timestamp, which is required if --time is set
          and must be omitted it otherwise, and MEM is in bytes.
      * The exception to the rule above is a file called "process.*".
          In this file the lines must be TIME#MEM
          where TIME# is always required and MEM is, again, in bytes.
    """
    # Process lines and store them in a temporary file.
    marks = _Markers()
    temps = []
    peaks = []
    for profile in args.files:
        thread = os.path.basename(profile).rsplit(".", 1)[0]
        if thread == "process":
            if args.no_process:
                continue
            print("Processing data for the process", file=sys.stderr)
            with open(profile) as f:
                data = tempfile.NamedTemporaryFile(mode="w")
                for line in f:
                    line = line.rstrip()
                    try:
                        (time, mem) = _parse_process_memory(line)
                        mem = int(mem) / 1024
                    except ValueError:
                        print("Unable to parse a line", file=sys.stderr)
                        continue
                    mem -= args.process_rebase
                    data.write("{0} {1}\n".format(time, mem))
                temps.append((data, "Process"))
            data.flush()
            continue
        print("Processing data for thread " + thread, file=sys.stderr)
        with open(profile) as f:
            zero_insert = False  # Used to remove duplicate zero points after a non-zero point.
            prev_zero = None  # A zero point should be added before the current point:
                              #   if there is a sequence of zero values we should plot the
                              #   first and last zeros but not the ones in the middle.
                              #   The first is needed to prevent misleading graphs, the last
                              #   is to prevent strange lines cutting the graph.
            index = 0  # Used to convert file:line:function to a number.
                       # Although file:line:function has more meaning, it is impossible to see in the plot.
            data = tempfile.NamedTemporaryFile(mode="w")
            for line in f:
                line = line.rstrip()
                try:
                    (time, name, mem) = _parse_thread_memory(line, args.time)
                    mem = int(mem)
                except ValueError:
                    print("Unable to parse a line.", file=sys.stderr)
                    continue
                if mem or zero_insert:
                    if prev_zero:
                        data.write('{0} {1}\n'.format(prev_zero or index, 0))
                        prev_zero = None
                    kmem = mem / 1024  # Convert B to KB
                    if args.cap and abs(kmem) > args.cap:
                        kmem = math.copysign(args.cap, kmem)
                    data.write('{0} {1}\n'.format(time or index, kmem))
                    if abs(kmem) > args.peak:
                        label = "{0}=>{1}".format(name, mem)
                        peaks.append((time or index, kmem, marks.newMark(label, thread)))
                    zero_insert = kmem != 0
                    index += 1
                else:
                    prev_zero = time
            if index:
                data.flush()
                temps.append((data, thread))
            else:
                data.close()
    # If time is available sort all peaks and filter the list to avoid overlaps.
    if args.time and peaks:
        sorted_peaks = sorted(peaks)
        peaks = [sorted_peaks[0]]
        for (time, mem, mark) in sorted_peaks[1:]:
            (ptime, pmem, pmark) = peaks[-1]
            if (abs(time - ptime) > args.peak_delta_time or
                abs(mem - pmem) > args.peak_delta_value):
                peaks.append((time, mem, mark))
    # Create plot definition.
    plot = tempfile.NamedTemporaryFile(mode="w")
    legend = open("memg.txt", "w")
    plot.write('set term svg size 1920,1080\n')
    plot.write('set output "memg.svg"\n')
    if args.time:
        plot.write('set xdata time\n')
        plot.write('set timefmt "%s"\n')
        tfrom = _parse_datetime(args.time_from)
        tto = _parse_datetime(args.time_to)
        if tfrom or tto:
            plot.write('set xrange [{0}:{1}]\n'.format(tfrom, tto))
    for (i, v, m) in peaks:
        plot.write('set label at "{0}",{1} "{2}" front center\n'.format(i, v, m))
    for m in sorted(marks.iter()):
        legend.write('{0}: {1}\n'.format(m, marks.getElement(m)))
    plot.write('plot ')
    for (temp, thread) in temps[:-1]:
        plot.write('"{0}" using 1:2 with lines title "{1}", \\\n'
                   .format(temp.name, thread))
    if temps:
        plot.write('"{0}" using 1:2 with lines title "{1}"\n'
                   .format(temps[-1][0].name, temps[-1][1]))
    # Create plot.
    plot.flush()
    print("Running gnuplot.", file=sys.stderr)
    gnuplot = subprocess.Popen(["gnuplot", plot.name])
    gnuplot.wait()
    for (temp, _) in temps:
        temp.close()
    plot.close()
    legend.close()


def memh(args):
    """Highlights most allocating and freeing functions.

    Converts the given files into a gnuplot compatible format
    then calls gnuplot to create the graph.
    Also creates a legend file that names the displayed functions.

    The files must meet the following assumptions:
      * Each line in non-empty files has the form TIME#NAME=>MEM
          where TIME# is a Unix timestamp, which is required if --time is set
          and must be omitted it otherwise, and MEM is in bytes.
      * The exception to the rule above is a file called "process.*" which is ignored.
    """
    # Build bins.
    bins = {}
    for profile in args.files:
        thread = os.path.basename(profile).rsplit(".", 1)[0]
        if thread == "process":
            continue
        print("Processing data for thread " + thread, file=sys.stderr)
        with open(profile) as f:
            for line in f:
                line = line.rstrip()
                (time, name, mem) = _parse_thread_memory(line, args.time)
                mem = int(mem) / 1024
                bins[name] = bins.get(name, 0) + mem
    # Write them to file.
    def key(kv):
        (k, v) = kv
        return (v, k)
    histo = sorted(bins.items(), key=key)
    data = tempfile.NamedTemporaryFile(mode="w")
    marks = _Markers()
    for (name, mem) in histo[:30]:
        data.write('"{0}" {1}\n'.format(marks.newMark(name), mem))
    for (name, mem) in histo[-30:]:
        data.write('"{0}" {1}\n'.format(marks.newMark(name), mem))
    data.flush()
    # Create plot definition.
    plot = tempfile.NamedTemporaryFile(mode="w")
    plot.write('set term svg size 1920,1080\n')
    plot.write('set output "memh.svg"\n')
    plot.write('plot "{0}" using 2:xticlabels(1) with boxes\n'
               .format(data.name))
    # Write legend.
    legend = open("memh.txt", "w")
    for m in sorted(marks.iter()):
        legend.write('{0}: {1}\n'.format(m, marks.getElement(m)))
    legend.close()
    # Create plot.
    plot.flush()
    print("Running gnuplot.", file=sys.stderr)
    gnuplot = subprocess.Popen(["gnuplot", plot.name])
    gnuplot.wait()
    plot.close()
    data.close()


def nesting(args):
    """Display per-thread stack nesting.

    Converts the given files into a gnuplot compatible format
    then calls gnuplot to create the graph.

    The files must meet the following assumptions:
      * Each line in non-empty files has the form SPACESTIME#.*
          where SPACES is a sequence of spaces, one for each stack level,
          TIME# is a Unix timestamp, which is required if --time is set
          and must be omitted it otherwise, and .* is anything (and is ignored).
    """
    temps = []
    for profile in args.files:
        thread = os.path.basename(profile).rsplit(".", 1)[0]
        print("Processing data for thread " + thread, file=sys.stderr)
        with open(profile) as f:
            data = tempfile.NamedTemporaryFile(mode="w")
            index = 0
            for line in f:
                line = line.rstrip()
                (level, time, name) = _parse_thread_stack(line, args.time)
                data.write("{0} {1}\n".format(time if time else index, level))
                index += 1
            if index:
                data.flush()
                temps.append((data, thread))
            else:
                data.close()
    # Create plot definition.
    plot = tempfile.NamedTemporaryFile(mode="w")
    plot.write('set term png size 1920,1080\n')
    plot.write('set output "nesting.png"\n')
    if args.time:
        plot.write('set xdata time\n')
        plot.write('set timefmt "%s"\n')
    plot.write('plot ')
    for (temp, thread) in temps[:-1]:
        plot.write('"{0}" using 1:2 with points title "{1}", \\\n'
                   .format(temp.name, thread))
    plot.write('"{0}" using 1:2 with points title "{1}"\n'
               .format(temps[-1][0].name, temps[-1][1]))
    # Create plot.
    plot.flush()
    print("Running gnuplot.", file=sys.stderr)
    gnuplot = subprocess.Popen(['gnuplot', plot.name])
    gnuplot.wait()
    for (temp, _) in temps:
        temp.close()
    plot.close()


def interleave(args):
    """Graph the interleaving of threads.

    Converts the given files into a gnuplot compatible format
    then calls gnuplot to create the graph.
    Also creates a legend file that names the displayed threads.

    The files must meet the following assumptions:
      * Each line in non-empty file has the form TIME#.*
          where TIME# is a Unix timestamp and .* is ignored.
      * The exception to the rule above is a file called "process.*" which is ignored.
    """
    def fold(items):
        """Removes all consecutive thread events except the first and last."""
        final = []
        if items:
            final.append(items[0])
            (ltime, lthread) = items[0]
            for (time, thread) in items[1:]:
                if thread != lthread:
                    final.append((ltime, lthread))
                    final.append((time, thread))
                ltime = time
                lthread = thread
        return final

    def split(items):
        """Split a list of items into multiple lists, one per thread."""
        final = {}
        for (time, thread) in items:
            if thread not in final:
                final[thread] = [time]
            else:
                final[thread].append(time)
        return final

    times = []
    threads = {}
    thread_id = 0
    for profile in args.files:
        thread = os.path.basename(profile).rsplit(".", 1)[0]
        if thread == "process":
            continue
        threads[thread] = thread_id
        thread_id += 1
        print("Processing data for thread " + thread, file=sys.stderr)
        with open(profile) as f:
            for line in f:
                line = line.rstrip()
                (time, _, _) = _parse_thread_memory(line, True)
                times.append((time, thread))
                used = True
    times = split(fold(sorted(times)))
    temps = []
    for (thread, stamps) in times.items():
        data = tempfile.NamedTemporaryFile(mode="w")
        for stamp in stamps:
            data.write('{0} {1}\n'.format(stamp, threads[thread]))
        data.flush()
        temps.append((data, thread))
    # Create plot definition.
    plot = tempfile.NamedTemporaryFile(mode="w")
    legend = open("interleave.txt", "w")
    plot.write('set term png size 1920,1080\n')
    plot.write('set output "interleave.png"\n')
    plot.write('set xdata time\n')
    plot.write('set timefmt "%s"\n')
    tfrom = _parse_datetime(args.time_from)
    tto = _parse_datetime(args.time_to)
    if tfrom or tto:
        plot.write('set xrange [{0}:{1}]\n'.format(tfrom, tto))
    plot.write('plot ')
    for (temp, thread) in temps[:-1]:
        plot.write('"{0}" using 1:2 with points title "{1}", \\\n'
                   .format(temp.name, thread))
    plot.write('"{0}" using 1:2 with points title "{1}"\n'
               .format(temps[-1][0].name, temps[-1][1]))
    for tname in sorted(threads.keys()):
        legend.write("{1}: {0}\n".format(tname, threads[tname]))
    # Create plot.
    plot.flush()
    print("Running gnuplot.", file=sys.stderr)
    gnuplot = subprocess.Popen(['gnuplot', plot.name])
    gnuplot.wait()
    for (temp, _) in temps:
        temp.close()
    plot.close()
    legend.close()


def decorate_stack(args):
    """Decorates data from a stack trace with memory information."""
    is_event_line_number = False
    max_line = 0
    try:
        max_line = int(args.event)
        is_event_line_number = True
    except ValueError:
        pass
    print("Scanning memory file looking for the event.", file=sys.stderr)
    mem_to_reverse = tempfile.NamedTemporaryFile()
    event_file_name = ""
    event_function_name = ""
    mem_time = 0
    with open(args.mem) as f:
        index = 0
        for line in f:
            line = line.rstrip()
            mem_to_reverse.write("{0}\n".format(line))
            index += 1
            if ((is_event_line_number and index >= max_line) or
                line == args.event):
                break;
        mem_to_reverse.flush()
        (mem_time, name, _) = _parse_thread_memory(line, True)
        (event_file_name, _, event_function_name) = name.split(":")
    print("Scanning stack file looking for a matching event.", file=sys.stderr)
    stack_to_analize = tempfile.NamedTemporaryFile()
    with open(args.stack) as f:
        looking_for_start = True
        base_trace_level = 0
        for line in f:
            line = line.rstrip()
            (level, stack_time, name) = _parse_thread_stack(line, True)
            (file_name, _, function_name) = name.split(":")
            if looking_for_start and stack_time > mem_time:
                    raise Exception("Unable to find event in stack trace.")
            # When a matching event before mem_time is found assume it is
            # the latest and start writing lines to the temp file.
            # If it was not, the next match will reset the temp file and
            # overwrite false positives.
            if (event_file_name == file_name and
                    event_function_name == function_name and
                    stack_time <= mem_time):
                base_trace_level = level
                looking_for_start = False
                data = line[base_trace_level:]
                stack_to_analize.seek(0);
                stack_to_analize.write("{0}\n".format(data))
                stack_to_analize.truncate();
            elif not looking_for_start:
                if level <= base_trace_level:
                    # A trace event outside the scope of interest was found.
                    break
                else:
                    data = line[base_trace_level:]
                    stack_to_analize.write("{0}\n".format(data))
        stack_to_analize.flush()
    print("Reversing memory events of interest.", file=sys.stderr)
    reversed_mem = tempfile.NamedTemporaryFile()
    reverse = subprocess.Popen([args.reverse, mem_to_reverse.name], stdout=reversed_mem)
    reverse.wait()
    mem_to_reverse.close()
    print("Parsing stack trace into a tree.", file=sys.stderr)
    stack_to_analize.seek(0)
    reversed_mem.seek(0)

    def attach_memory_line(node):
        node.store("mem-line", reversed_mem.readline().rstrip())

    def print_decorate_trace(node):
        mem_line = node.get("mem-line")
        (end_time, end_name, mem) = _parse_thread_memory(mem_line, True)
        (_, start_time, start_name) = _parse_thread_stack(node.value(), True)
        (file_name, end_line, function_name) = end_name.split(":")
        (_, start_line, _) = start_name.split(":")
        delta = end_time - start_time
        indent = "".join([args.indent] * node.level())
        if args.prefix and file_name.startswith(args.prefix):
            file_name = file_name[len(args.prefix):]
        print("{0}{1}@{2}:{3}-{4}, Time: {5} s, Memory: {6} B".format(
            indent, function_name, file_name, start_line, end_line, delta, mem))

    tree = StackTree.build_from_file(stack_to_analize)
    print("Reconciling trace and memory.", file=sys.stderr)
    tree.reverse_traverse(attach_memory_line)
    print("Decorating trace.", file=sys.stderr)
    tree.traverse(print_decorate_trace)
    # Clean up.
    reversed_mem.close()
    stack_to_analize.close()


# Command line parsers.
def _common_parser(parser, function=None):
    """Populates a parser with the generic command options."""
    parser.add_argument("files", metavar="FILE", nargs="+", help="The dump files to process.")
    if function:
        parser.set_defaults(process=function)

def _time_parser(parser):
    parser.add_argument("--time_from", action="store", default=None,
                        help="Initial time for the range passed to gnuplot.")
    parser.add_argument("--time_to", action="store", default=None,
                        help="Final time for the range passed to gnuplot.")


def _interleave_parser(parser):
    """Populates a parser with the memg command options."""
    _time_parser(parser)
    _common_parser(parser)
    parser.set_defaults(process=interleave)


def _memg_parser(parser):
    """Populates a parser with the memg command options."""
    def parse_delta(time):
        return int(time) * 1000

    parser.add_argument("--cap", action="store", default=None, type=int,
                        help="Caps memory peaks to the given value.")
    parser.add_argument("--no_process", action="store_true", default=False,
                        help="Ignore process-wide memory data if available.")
    parser.add_argument(
        "--process_rebase", action="store", default=30000, type=int,
        help=("Reduce the process memory by the given amount "
        "(in KB). Helps to see small memory fluctuations in threads."))
    parser.add_argument(
        "--peak", action="store", default=200, type=int,
        help="Memory deltas exceeding this size (in KB) are considered peaks.")
    parser.add_argument(
        "--peak_delta_time", action="store", default=60, type=parse_delta,
        help=("Prevent two peeks too close in time to be marked. Helps keep "
              "the graphs readable."))
    parser.add_argument(
        "--peak_delta_value", action="store", default=500, type=int,
        help=("Prevent two peeks too close in memory to be marked. Helps keep "
              "the graphs readable."))
    _time_parser(parser)
    _common_parser(parser)
    parser.set_defaults(process=memg)


def _decorate_stack_parser(parser):
    parser.add_argument(
        "--indent", action="store", default=" ",
        help="String used to indent trace levels.")
    parser.add_argument(
        "--prefix", action="store", default=None,
        help="File names prefix to omit.")
    parser.add_argument(
        "--reverse", action="store", default="tac",
        help="Command used to reverse useful memory file portion.")
    parser.add_argument("mem", action="store", help="Memory dump file.")
    parser.add_argument("stack", action="store", help="Stack dump file.")
    parser.add_argument("event", action="store", help=(
        "Memory event to look for. This is either a line number or a full "
        "line in the dump file."))
    parser.set_defaults(process=decorate_stack)


def main():
    parser = argparse.ArgumentParser(
        description="ThreadGraph dumps post-processor and visualizer.")
    parser.add_argument(
        "--no_time", action="store_true", default=False,
        help="Indicates that the dump files do not contain time information.")
    subparsers = parser.add_subparsers(help="Type of processing to do.")

    _memg_parser(subparsers.add_parser("memg", help="Graph per-thread memory profile."))
    _common_parser(subparsers.add_parser(
        "memh", help=("Find functions with highest memory allocation and "
                     "deallocation.")), memh)
    _common_parser(subparsers.add_parser(
        "nesting", help="Visualize stack trace nesting."), nesting)
    _interleave_parser(subparsers.add_parser(
        "interleave", help="Visualize thread interleaving."))
    _decorate_stack_parser(subparsers.add_parser(
        "decorate-stack", help=("Decorate stack traces with the help of "
                                "memory information.")))

    args = parser.parse_args()
    args.time = not args.no_time
    args.process(args)


if __name__ == "__main__":
    main()
