ThreadGraph memory profiler
===========================
ThreadGraph is a Python memory profiler for multi-threaded applications.

It is a two parts project: the first part is a module to include in the project
being profiled and the second is a command line tool to process the data
generate by the process.


Requirements
------------
The profiling module has the following requirements:

  * Python 2.6 or later (tested with 2.6, 2.7, 3.4rc1)
  * Pympler

While the processing module has the following:

  * Python 2.7 or later (it uses argparse)
  * gnuplot


Tutorial
========
This sections goes through the process of profiling a cherrypy based
application.

Enabling the profiler
---------------------
The first step is to include and enable the memory profiler.
The only portion of code that needs to be changed in the profiled program is
the main function, so here is a pretend main for a pretend application:

    def main():
        auth_app = cherrypy.tree.mount(Server(), '/')
        cherrypy.engine.start()
        cherrypy.engine.block()

    if __name__ == "__main__":
        main()

To start collectiond data change main to this:

    from thread_graph import ProcessProfiler

    def main():
        profiler = ProcessProfile(default_log_path="/path/to/profile/data", profile="python")
        profiler.setFilter("/path/to/project/root")
        #profiler.enableForkedProfile()
        profiler.trackStack()
        profiler.enable()
        auth_app = cherrypy.tree.mount(Server(), '/')
        # ...

First of all this will save profile data into "/path/to/profile/data/$pid"
Next there are some common, important, options:

  * **profile**: the profile keyword argument indicates the type of events to
                 profile, valid options are "python", "c" or "both" (default).
  * **setFilter**: this method sets the prefix filter applied to each event.
                   The argument is the absolute path/file containing the code
                   to profile.
  * **enableForkedProfile**: if your process creates subprocesses (forks in
                             Unix terminology) and you wish to profile the
                             subprocesses as well, this option allows exactly that.
  * **trackStack**: produce stack trace information along side memory information.
  * **enable**: start profiling the thread.


These are the options that are used in this tutorial and should be the most used.
The full documentation is stored as Python documentation strings and is extracted
into the _docs_ directory by _pydoc_.

Once your process is set up to enable the profiler you can start it as you
normally would and it is now time to collect information.
If you have a particular task to profile go on and perform it as you normally
would (but with a little more patient).
Otherwise, if you do not have a specific task in mind and are just curious to
see how and what your program is doing simply go nuts!

### A problem with performance
Although it is technically possible to profile every event in your program
regardless of source or type of event, it is an highly discouraged approach.
Everything means that builtin C functions and standard python modules are
profiled as well.
This leads to a complete image of your process but at the cost of an un-usably
slow process and enormous dump files.


Processing the dumps
--------------------
You run your program with the profiler enabled and collect gigs of data.
Finally you can figure out what is going on, but how?!?

While the profiler runs it outputs data into files for later processing.
The first thing to understand is the structure of these files.
In the set up you specified a base path to use ("/path/to/profile/data").
Every time you run the process it will create a directory in that path
named after the process id of the running instance.
Inside that directory files, usually one or more per thread, are created.

The result looks something like this:
    /path/to/profile/data
        /6685
            /process.mem
            /MainThread.mem
            /MainThread.stack
            /Thread-1.mem
            /Thread-1.stack
            ...
The _process.mem_ file stores a process level, timestamped, memory trace.
Than for each thread a memory dump file and a stack trace file (if enabled)
are created.
Those files can be processed with the ProfilerGraph command line utility
to extract information and produce memory usage graphs.


### Peaks
You should probably start by looking for memory spikes since they are
easy to find quickly.
This is because the ProfilerGraph has a command to find them for you:

    python ProfilerGraph memg /path/to/profile/data/pid/*.mem

The above command produces an output similar to the following to
stdout and the _memg.svg_ and _memg.txt_ files.

    Processing data for thread CP Server Thread-5
    Processing data for thread CP Server Thread-6
    Processing data for thread CP Server Thread-7
    Processing data for thread CP Server Thread-8
    Processing data for thread CP Server Thread-9
    Processing data for thread HTTPServer Thread-4
    Processing data for thread Thread-1
    Processing data for thread Thread-2
    Processing data for thread Thread-45
    Processing data for thread Thread-46
    Processing data for thread MainThread
    Processing data for the process
    Running gnuplot.

This command produces a memory graph (memg) that shows the amount of
memory allocated by each thread, compared to the process level memory usage.

This is an example of graph produced by the command above:

    ![memg.svg](memg.svg)

There is a lot to talk about here:

    * Axis:
    * Legend:
    * The process memory:
    * The threads memory:
    * Peaks:
