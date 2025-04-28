help:
	@echo "make build                Build crossdb library and tool"
	@echo "make debug                Build crossdb library and tool with debug"
	@echo "make run                  Run crossdb tool"
	@echo "make clean                Clean build result"
	@echo "make install              Install crossdb(lib&tool&header) to Linux/MacOS/FreeBSD"
	@echo "make uninstall            Uninstall crossdb from Linux/MacOS/FreeBSD"
	@echo "make example              Build and run example (need to install crossdb first)"
	@echo "make smoketest            Build and run smoke test (need to install crossdb first)"
	@echo "make bench                Build and run bench test (need to install crossdb first)"
	@echo "make bench-sqlite         Build and run sqlite bench test (need to install sqlite3 first)"
	@echo "make bench-stlmap         Build and run C++ STL Map and HashMap(unordered_map) bench test"
	@echo "make bench-boostmidx      Build and run C++ Boost MultiIndex Order and Hash bench test"

.PHONY: build
build:
ifeq ($(shell uname -s), Darwin)
else
	$(CC) -o build/libcrossdb.so -fPIC -shared -lpthread -O2 src/crossdb.c
endif
	$(CC) -o build/xdb-cli src/xdb-cli.c -O2 -lpthread
	cp include/crossdb.h build/

debug:
	$(CC) -o build/libcrossdb.so -fPIC -lpthread -shared -g -DXDB_DEBUG2 src/crossdb.c
	$(CC) -o build/xdb-cli src/xdb-cli.c -lpthread -g
	cp include/crossdb.h build/

smoketest:
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
	$(CC) -o build/xdb-cli src/xdb-cli.c -lpthread -fsanitize=address -g
	gdb build/xdb-cli

install:
	@mkdir -p /usr/local/lib/
	@mkdir -p /usr/local/bin/
	install -c build/xdb-cli /usr/local/bin/
ifeq ($(shell uname -s), Darwin)
	$(CC) -o /usr/local/lib/libcrossdb.dylib -dynamiclib -lpthread -O2 src/crossdb.c
	install -c build/crossdb.h $(shell xcrun --show-sdk-path)/usr/include
else
	@mkdir -p /usr/local/include/
	install -c build/crossdb.h /usr/local/include/
	install -c build/libcrossdb.so /usr/local/lib/
	ldconfig
endif

uninstall:
	rm -rf /usr/local/bin/xdb-cli
ifeq ($(shell uname -s), Darwin)
	rm -rf /usr/local/lib/libcrossdb.dylib
	rm -rf $(shell xcrun --show-sdk-path)/usr/include/crossdb.h
else
	rm -rf /usr/local/lib/libcrossdb.so
	rm -rf /usr/local/include/crossdb.h
endif

example:
	make -C examples/c/

.PHONY: bench
bench:
	make -C bench/basic/

bench-sqlite:
	make -C bench/basic/ sqlite

bench-stlmap:
	make -C bench/basic/ stlmap

bench-boostmidx:
	make -C bench/basic/ boostmidx
