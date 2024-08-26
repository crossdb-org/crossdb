all:
	$(CC) -o bench-crossdb bench-crossdb.c -O2 -lcrossdb -lpthread 
	./bench-crossdb

debug:
	$(CC) -o bench-crossdb bench-crossdb.c ../../src/crossdb.c -g -fsanitize=address
	./bench-crossdb

sqlite:
	$(CC) -o bench-sqlite bench-sqlite.c -O2 -lsqlite3 
	./bench-sqlite

fast:
	$(CC) -o bench-crossdb bench-crossdb.c ../../src/crossdb.c -O3 -march=native
	./bench-crossdb

clean:
	rm -f a.out bench-crossdb bench-sqlite
