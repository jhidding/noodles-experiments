from noodles import gather, schedule, run_process, serial
import multiprocessing as mp
from queue import Empty 
import os
import time
import utilities_cython as uc 
from utilities_python import sumPrimes_noodles

def worker(q):
    while True:
        try:
            x = q.get(block=False)
            print(uc.sumPrimes(x))
        except Empty:
            break

if __name__ == "__main__":

    range_of_values = range(int(4e6), int(5e6), int(2e5))  
    # range_of_values = range_of_values[-1]
    ncpus = mp.cpu_count() 

    print("First multiprocessing ")
    start_mp = time.time()
    my_q  = mp.Queue()
    for i in range_of_values:
        my_q.put(i)
    procs=[mp.Process(target=worker, args=(my_q,))  for i in range(ncpus)]
    for ps in procs:
        ps.start()
    for ps in procs:
        ps.join()
    end_mp = time.time()
    print()
    print("Now serial processing ")

    start_sp  = time.time()
    for i in range_of_values:
        print(uc.sumPrimes(i))
    end_sp = time.time()

    print()
    print("Now processing using Noodles ")
    start_noodles = time.time()
     
    result = run_process(gather(*[schedule(sumPrimes_noodles)(x) for x in range_of_values]), n_processes = ncpus, registry = serial.base)
    print(result)
    end_noodles = time.time()

    print("multiprocessing takes ", end_mp - start_mp,      " seconds")
    print("single thread takes ", end_sp - start_sp,  "   seconds")
    print("Noodles takes ", end_noodles - start_noodles,  "   seconds")

