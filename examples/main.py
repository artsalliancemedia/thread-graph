# Fix import path to include parent dir.
import os
import sys
sys.path.append(os.path.abspath(".."))

import random
import threading
import time

from Profiler import ProcessProfile


# Constants used to scale tests.
THREAD_COUNT = 8
THREAD_SIZE = 128 * 1024
RAND_TIME = 10


# A dummy thread that allocates 
class DummyThread(threading.Thread):
    def before(self):
        time.sleep(random.randint(0, RAND_TIME / 5))

    def after(self):
        time.sleep(random.randint(0, RAND_TIME / 5))

    def doStuff(self):
        self.nums = []
        num_count = THREAD_SIZE / 16  # 16 = Min size of an integer.

        while num_count > 0:
            print("%d numbers left to allocate" % num_count)
            if num_count > 2:
                nums = random.randint(2, num_count)
            else:
                nums = 2
            self.nums.extend([i % 16384 for i in xrange(nums)])
            num_count -= nums
            time.sleep(random.randint(0, RAND_TIME))

    def run(self):
        self.before()
        self.doStuff()
        self.after()


def main():
    profiler = ProcessProfile(default_log_path="/data/profiling/example", profile="python")
    #profiler.enableForkedProfile()
    profiler.trackStack()
    profiler.setProcessMemoryFrequence(20)
    profiler.enable()
    threads = [DummyThread() for _ in xrange(THREAD_COUNT)]
    [t.start() for t in threads]
    [t.join()  for t in threads]


if __name__ == "__main__":
    main()
