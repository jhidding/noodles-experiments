#!/usr/bin/env python

from noodles import (
    gather, schedule, run_process, serial, run_parallel)

import multiprocessing
import threading
import queue
from queue import Empty
import time
from utilities_cython import sum_primes
from utilities_python import sumPrimes_noodles


def worker(q):
    while True:
        try:
            x = q.get(block=False)
            print(sumPrimes_noodles(x))
        except Empty:
            break


if __name__ == "__main__":

    range_of_values = range(int(1e6), int(2e6), int(5e4))
    ncpus = multiprocessing.cpu_count()

    print("First, one thread.")
    start_st = time.time()
    for i in range_of_values:
        print(sum_primes(i))
    end_st = time.time()
    print()

    print("Next, as many threads as there are logical cores, taken from multiprocessing.cpu_count().")
    start_mt = time.time()
    my_q = queue.Queue()
    for i in range_of_values:
        my_q.put(i)
    procs = [
        threading.Thread(target=worker, args=(my_q,))
        for i in range(ncpus)]
    for ps in procs:
        ps.start()
    for ps in procs:
        ps.join()
    end_mt = time.time()

    print()
    print("Now Noodles with as many threads as there are logical cores.")
    start_noodles = time.time()
    result = run_parallel(
        gather(*(schedule(sumPrimes_noodles)(x) for x in range_of_values)),
        n_threads=ncpus)
    print(result)
    end_noodles = time.time()

    print("A single thread takes {0:.2f} seconds".format(end_st - start_st))
    print("Multithreading takes {0:.2f} seconds".format(end_mt - start_mt))
    print("Noodles takes {0:.2f} seconds".format( end_noodles - start_noodles))
