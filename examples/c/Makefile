all:
	$(CC) -o example example.c -lcrossdb -lpthread
	./example

debug:
	$(CC) -o example example.c ../../src/crossdb.c -lpthread -g -fsanitize=address
	./example

gdb:
	$(CC) -o example example.c ../../src/crossdb.c -lpthread -g -fsanitize=address
	gdb example

clean:
	rm -f a.out example 