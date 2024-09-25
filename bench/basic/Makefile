all:
	$(CC) -o bench-crossdb.bin bench-crossdb.c -O2 -lcrossdb -lpthread 
	./bench-crossdb.bin

debug:
	$(CC) -o bench-crossdb.bin bench-crossdb.c ../../src/crossdb.c -lpthread -g -fsanitize=address

gdb: debug
	gdb ./bench-crossdb.bin

sqlite:
	$(CC) -o bench-sqlite.bin bench-sqlite.c -O2 -lsqlite3 -lpthread
	./bench-sqlite.bin

stlmap:
	$(CC) -o bench-stlmap.bin bench-stlmap.cpp -std=c++17 -O2 -lpthread -lstdc++ -lm
	./bench-stlmap.bin

boostmidx:
	$(CC) -o bench-boostmidx.bin bench-boostmidx.cpp -std=c++17 -O2 -lpthread -lstdc++ -lm
	./bench-boostmidx.bin

fast:
	$(CC) -o bench-crossdb.bin bench-crossdb.c ../../src/crossdb.c -O3 -march=native -lpthread
	./bench-crossdb.bin

build:
	$(CC) -o bench-crossdb.bin bench-crossdb.c -O2 -lcrossdb -lpthread 
	$(CC) -o bench-sqlite.bin bench-sqlite.c -O2 -lsqlite3 -lpthread 
	$(CC) -o bench-stlmap.bin bench-stlmap.cpp -std=c++17 -O2 -lpthread -lstdc++ -lm
	$(CC) -o bench-boostmidx.bin bench-boostmidx.cpp -std=c++17 -O2 -lpthread -lstdc++ -lm

test: build
	@echo
	@echo "\n***************** CrossDB *****************\n" 
	@./bench-crossdb.bin -q -j -r 5 -n 1000
	@./bench-crossdb.bin -q -j -r 5 -n 10000
	@./bench-crossdb.bin -q -j -r 5 -n 100000
	@./bench-crossdb.bin -q -j -r 5 -n 1000000
	@./bench-crossdb.bin -q -j -r 5 -n 10000000

	@echo
	@echo "\n***************** STL Map & HashMap *****************\n"
	@./bench-stlmap.bin -q -j -r 5 -n 1000
	@./bench-stlmap.bin -q -j -r 5 -n 10000
	@./bench-stlmap.bin -q -j -r 5 -n 100000
	@./bench-stlmap.bin -q -j -r 5 -n 1000000
	@./bench-stlmap.bin -q -j -r 5 -n 10000000

	@echo
	@echo "\n***************** SQLite *****************\n"
	@./bench-sqlite.bin -q -j -r 5 -n 1000
	@./bench-sqlite.bin -q -j -r 5 -n 10000
	@./bench-sqlite.bin -q -j -r 5 -n 100000
	@./bench-sqlite.bin -q -j -r 5 -n 1000000
	@./bench-sqlite.bin -q -j -r 5 -n 10000000

clean:
	rm -f a.out *.bin *.txt
