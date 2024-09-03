all:
	$(CC) -o bench-crossdb bench-crossdb.c -O2 -lcrossdb -lpthread 
	./bench-crossdb

debug:
	$(CC) -o bench-crossdb bench-crossdb.c ../../src/crossdb.c -lpthread -g -fsanitize=address

gdb: debug
	gdb ./bench-crossdb

sqlite:
	$(CC) -o bench-sqlite bench-sqlite.c -O2 -lsqlite3 -lpthread
	./bench-sqlite

stlmap:
	$(CC) -o bench-stlmap bench-stlmap.cpp -std=c++17 -O2 -lpthread -lstdc++ -lm
	./bench-stlmap

fast:
	$(CC) -o bench-crossdb bench-crossdb.c ../../src/crossdb.c -O3 -march=native -lpthread
	./bench-crossdb

build:
	$(CC) -o bench-crossdb bench-crossdb.c -O2 -lcrossdb -lpthread 
	$(CC) -o bench-sqlite bench-sqlite.c -O2 -lsqlite3 -lpthread 
	$(CC) -o bench-stlmap bench-stlmap.cpp -std=c++17 -O2 -lpthread -lstdc++ -lm

test: build
	@echo
	@echo "\n***************** CrossDB *****************\n" 
	@./bench-crossdb -q -j -r 5 -n 1000
	@./bench-crossdb -q -j -r 5 -n 10000
	@./bench-crossdb -q -j -r 5 -n 100000
	@./bench-crossdb -q -j -r 5 -n 1000000
	@./bench-crossdb -q -j -r 5 -n 10000000

	@echo
	@echo "\n***************** STL Map & HashMap *****************\n"
	@./bench-stlmap -q -j -r 5 -n 1000
	@./bench-stlmap -q -j -r 5 -n 10000
	@./bench-stlmap -q -j -r 5 -n 100000
	@./bench-stlmap -q -j -r 5 -n 1000000
	@./bench-stlmap -q -j -r 5 -n 10000000

	@echo
	@echo "\n***************** SQLite *****************\n"
	@./bench-sqlite -q -j -r 5 -n 1000
	@./bench-sqlite -q -j -r 5 -n 10000
	@./bench-sqlite -q -j -r 5 -n 100000
	@./bench-sqlite -q -j -r 5 -n 1000000
	@./bench-sqlite -q -j -r 5 -n 10000000

clean:
	rm -f a.out bench-crossdb bench-sqlite bench-stlmap *.txt
