all:
	$(CC) bench.c -O2 -lcrossdb -lpthread 
	./a.out

fast:
	$(CC) bench.c ../../src/crossdb.c -O3 -march=native
	./a.out

clean:
	rm -f a.out