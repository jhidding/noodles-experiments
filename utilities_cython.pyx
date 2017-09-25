from libc.math cimport ceil, sqrt


cdef inline int primeQ(int n) nogil:
   """return a boolean, is the input integer a prime?"""
   if n == 2:
       return True
   cdef int max_i = <int>ceil(sqrt(n))
   cdef int i = 2
   while i <= max_i:
      if n % i == 0:
          return False
      i += 1
   return True

cpdef unsigned long sumPrimes(int n) nogil:
   """return sum of all primes less than n """
   cdef unsigned long i = 0
   cdef int x
   for x in range(2, n):
       if primeQ(x):
           i += x
   return i

