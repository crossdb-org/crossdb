all:
	$(CC) -o xdb-test xdb_test.c -O2 -lcrossdb -lpthread 
	./xdb-test

debug:
	$(CC) -o xdb-test xdb_test.c ../src/crossdb.c -g -fsanitize=address
	./xdb-test

gdb:
	$(CC) -o xdb-test xdb_test.c ../src/crossdb.c -g -fsanitize=address
	gdb xdb-test

clean:
	rm -f xdb-test
