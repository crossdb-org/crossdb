all:
	$(CC) -o example example.c -lcrossdb -lpthread
	./example

gdb:
	$(CC) -o example example.c ../../src/crossdb.c -lpthread -g
	gdb example

clean:
	rm -f a.out example 