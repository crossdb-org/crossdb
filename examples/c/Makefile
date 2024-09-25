all:
	$(CC) -o example.bin example.c -lcrossdb -lpthread
	./example.bin

debug:
	$(CC) -o example.bin example.c ../../src/crossdb.c -lpthread -g -fsanitize=address
	./example.bin

gdb:
	$(CC) -o example.bin example.c ../../src/crossdb.c -lpthread -g -fsanitize=address
	gdb example.bin

clean:
	rm -f a.out *.bin