all:
	$(CC) -o xdb_smoke_test.bin xdb_smoke_test.c -O2 -lcrossdb -lpthread 
	./xdb_smoke_test.bin

debug:
	$(CC) -o xdb_smoke_test.bin xdb_smoke_test.c ../src/crossdb.c -g -fsanitize=address
	./xdb_smoke_test.bin

gdb:
	$(CC) -o xdb_smoke_test.bin xdb_smoke_test.c ../src/crossdb.c -g -fsanitize=address
	gdb xdb_smoke_test.bin

clean:
	rm -rf *.bin testdb
