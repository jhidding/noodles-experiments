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

    print("First multiprocessing ")
    start_mp = time.time()
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
    end_mp = time.time()
    print()
    print("Now serial processing ")

    start_sp = time.time()
    for i in range_of_values:
        print(sum_primes(i))
    end_sp = time.time()

    print()
    print("Now processing using Noodles ")
    start_noodles = time.time()
    result = run_parallel(
        gather(*(schedule(sumPrimes_noodles)(x) for x in range_of_values)),
        n_threads=ncpus)
    print(result)
    end_noodles = time.time()

    print("multiprocessing takes ", end_mp - start_mp,      " seconds")
    print("single thread takes ", end_sp - start_sp,  "   seconds")
    print("Noodles takes ", end_noodles - start_noodles,  "   seconds")
