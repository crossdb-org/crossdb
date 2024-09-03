help:
	@echo "make build                Build crossdb library and tool"
	@echo "make debug                Build crossdb library and tool with debug"
	@echo "make run                  Run crossdb tool"
	@echo "make clean                Clean build result"
	@echo "make install              Install crossdb(lib&tool&header) to Linux/FreeBSD"
	@echo "make uninstall            Uninstall crossdb from Linux/FreeBSD"
	@echo "make installmac           Install crossdb(lib&tool&header) to MacOS"
	@echo "make uninstallmac         Uninstall crossdb from MacOS"
	@echo "make example              Build and run example (need to install crossdb first)"
	@echo "make bench                Build and run bench test (need to install crossdb first)"
	@echo "make bench-sqlite         Build and run sqlite bench test (need to install sqlite3 first)"
	@echo "make bench-stlmap         Build and run C++ STL Map and HashMap(unordered_map) bench test"

.PHONY: build
build:
	$(CC) -o build/libcrossdb.so -fPIC -shared -lpthread -O2 src/crossdb.c
	$(CC) -o build/xdb-cli src/xdb-cli.c -lpthread -O2
	cp include/crossdb.h build/

debug:
	$(CC) -o build/libcrossdb.so -fPIC -lpthread -shared -g src/crossdb.c
	$(CC) -o build/xdb-cli src/xdb-cli.c -lpthread -g
	cp include/crossdb.h build/

.PHONY: test
test:
	make -C test/

run:
	build/xdb-cli

clean:
	rm -rf build/*
	make -C examples/c/ clean
	make -C bench/basic/ clean
	make -C test/ clean

wall:
	$(CC) -o build/xdb-cli src/xdb-cli.c -lpthread -O2 -Wall

gdb:
	$(CC) -o build/xdb-cli src/xdb-cli.c -lpthread -g
	gdb build/xdb-cli

install:
	install -c build/libcrossdb.so /usr/local/lib/
	install -c build/crossdb.h /usr/local/include/
	install -c build/xdb-cli /usr/local/bin/
	ldconfig

uninstall:
	rm -rf /usr/local/lib/libcrossdb.so
	rm -rf /usr/local/include/crossdb.h
	rm -rf /usr/local/bin/xdb-cli

installmac:
	$(CC) -o build/libcrossdb.so -dynamiclib -lpthread -O2 src/crossdb.c
	install -c build/libcrossdb.so /usr/local/lib/
	install -c build/crossdb.h /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include
	install -c build/xdb-cli /usr/local/bin/

uninstallmac:
	rm -rf /usr/local/lib/libcrossdb.so
	rm -rf /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/crossdb.h
	rm -rf /usr/local/bin/xdb-cli

example:
	make -C examples/c/

.PHONY: bench
bench:
	make -C bench/basic/

bench-sqlite:
	make -C bench/basic/ sqlite

bench-stlmap:
	make -C bench/basic/ stlmap
