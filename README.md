<p align="center">
	<a href="https://crossdb.org">
	<img src="https://crossdb.org/assets/favicon.png">
	</a>
</p>
<p align="center">
	<strong>Ultra High-performance Lightweight Embedded and Server OLTP RDBMS✨</strong>
</p>

**CrossDB** is a ultra high-performance, lightweight embedded and server OLTP RDBMS. 
It is designed for high-performance scenarios where the main memory can hold the entire database.

## Features

- Supports multiple OS platforms: Linux, Windows, MacOS, FreeBSD, etc.
- Supports multiple CPU architectures: X86, ARM, PPC, MIPS, etc.
- Supports OnDisk, In-memory (IMDB), RamDisk, and Hybrid Storage.
- Supports the standard RDBMS model.
- Supports standard SQL and many extensions from MySQL.
- Supports multiple databases.
- Supports embedded and client-server modes (TBD).
- Supports primary keys and multiple secondary indexes.
- Supports HASH and RBTREE (TBD) indexes.
- Supports multi-column indexes.
- Supports exact match, leftmost match (TBD), and range match (TBD).
- Supports standard ACID transactions.
- Supports WAL for OnDisk storage (TBD).
- Supports multiple threads and multiple processes access.
- Supports table-level read-write locks.
- Supports Reader-Writer MVCC (write transaction doesn't block read transactions).
- Supports an embedded CrossDB shell with convenient auto-completion.
- Supports multi-statement APIs.
- Supports prepared statement APIs.
- Ultra high performance.
- Very simple: Simple header and library file.
- Zero config: No complex configuration, truly out-of-the-box.

## Use Cases

- High-frequency trading (OLTP).
- High-performance queries.
- High-performance data management: Use CrossDB OnDisk DB to store data on HDD/SSD/Flash.
- High-performance IMDB: Use CrossDB In-Memory DB to manage process runtime data, replacing STL or hand-written data structures.
- Use CrossDB RamDisk DB to support process restartability and in-service software upgrades (ISSU) easily.
- Use CrossDB as a super-fast cache DB.

## Build and Install

### Download code

```bash
git clone https://github.com/crossdb-org/crossdb.git
cd crossdb
```

### Linux/FreeBSD

```bash
make build
sudo make install
```

### MacOS

```bash
make build
sudo make installmac
```

### Windows

You need to install [MINGW64](https://www.mingw-w64.org/) to build.
Then set the `gcc` path to `system environment variables` `Path` and make sure `gcc` can run.

```
winbuild.bat
```

### CMake
```
cd build
cmake ..
make
sudo make install
```

## Contribution

This project is still in its early stages and currently lacks stability. We welcome the following contributions:

- **Language bindings**: `Python`, `Java`, `Go`, `CSharp`, `JavaScript`, `PHP`, etc.
- **Bug Reporting**: Identify and report any issues or bugs you encounter.
- **Testing**: Participate in testing to ensure the reliability and stability of the project.

Your contributions will be greatly appreciated and will help us make this project more robust and reliable.


## Reference

### 1,000,000 Rows Random Access Benchmark vs. SQLite

<p align="center">
	<a href="https://crossdb.org/blog/benchmark/crossdb-vs-sqlite3/">
	<img src="https://crossdb.org/images/crossdb-vs-sqlite.png">
	</a>
</p>

https://crossdb.org/blog/benchmark/crossdb-vs-sqlite3/

### 1,000,000 Rows Random Access Benchmark vs. C++ STL Map and HashMap

<p align="center">
	<a href="https://crossdb.org/blog/benchmark/crossdb-vs-stlmap/">
	<img src="https://crossdb.org/images/crossdb-vs-stlmap.png">
	</a>
</p>

https://crossdb.org/blog/benchmark/crossdb-vs-stlmap/

### SQL Statements

https://crossdb.org/sql/statements/

### APIs

https://crossdb.org/client/api-c/

### Tutorial

https://crossdb.org/get-started/tutorial/
