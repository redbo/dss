cdef extern from "sys/mman.h":
    void *mremap (void *addr, int old_size, int new_size, int flags)
    void *mmap(void *addr, int length, int prot, int flags, int fd, int off)
    int munmap(void *addr, int length)
    int PROT_READ, PROT_WRITE, MAP_SHARED, MAP_ANON

cdef extern from "stdint.h":
    ctypedef unsigned long atomic_t "uint64_t"

cdef class AtomicT:
    cdef atomic_t *data

    def __cinit__(self, init=0):
        self.data = <atomic_t *>mmap(NULL, sizeof(atomic_t),
                PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0)
        self.data[0] = init

    def __dealloc__(self):
        munmap(self.data, sizeof(atomic_t))

    cdef atomic_t value(self):
        return self.data[0]

    cdef atomic_t inc(self, atomic_t amt=1):
        self.data[0] += amt
        return self.data[0]
