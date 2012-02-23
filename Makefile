all:
	#cython atomic_t.pyx
	gcc -fPIC -I/usr/include/python2.7 --shared -o atomic_t.so atomic_t.c -lpython2.7
