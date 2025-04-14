<p align="center">
	<a href="https://crossdb.org">
	<img src="https://crossdb.org/assets/favicon.png">
	</a>
</p>
<p align="center">
	<strong>Ultra High-performance Lightweight Embedded and Server OLTP RDBMS✨</strong>
</p>

## Features

| Name  					| **CrossDB**
 ----                       | ----
 Description    			| Ultra High-performance Lightweight Embedded and Server OLTP RDBMS✨  
 Primary database model  	| Relational DBMS  
 Secondary database models	| Document store (TBD)<br>Key-value store(TBD)
 Website					| crossdb.org
 Technical documentation	| crossdb.org/intro
 Developer					| JC Wang
 Initial release			| 2023
 License					| Open Source, MPL
 Cloud-based only			| no
 Implementation language	| C
 Server operating systems	| Server-Less Mode<br>Embedded Server Mode<br>Standalone Server Mode<br>Linux/MacOS/Windows/FreeBSD<br>32-bit or 64-bit X86, ARM, PowerPC, MIPS, etc
 Data scheme				| yes
 Typing 					| yes<br>BOOL<br>TINYINT, SMALLINT, INT, BIGINT<br>TIMESTAMP<br>FLOAT, DOUBLE<br>CHAR, VARCHAR<br>BINARY, VARBINARY<br>INET(IPv4/IPv6 host and subnet), MAC address
 XML support				| no
 Secondary indexes			| yes<br>HASH, RBTREE
 SQL 						| yes, many extensions from MySQL
 APIs and other access methods		| Proprietary native APIs<br>Multi-statement APIs<br>Prepared statement APIs<br>JDBC (TBD)<br>ODBC (TBD)
 Supported programming languages	| C, C++, Python, GO, Rust<br>More bindings (TBD)
 Server-side scripts		| LUA (TBD)
 Triggers					| yes, native languages (TBD)
 Partitioning methods		| none
 Replication methods		| Source-replica replication (TBD)<br>Multi-source replication (TBD)<br>Logical Replication (TBD)
 Data Subscription			| yes (TBD)
 MapReduce 					| no
 Consistency concepts		| Immediate Consistency
 Foreign keys				| yes (TBD)
 TTL						| yes (TBD)
 Transaction concepts		| ACID
 Concurrency 				| yes<br>Table-level read-write locks<br>Row-level read-write locks (TBD)<br>Reader-Writer MVCC (write transaction doesn't block read transactions)<br>PostgreSQL-like MVCC (TBD)
 Durability 				| yes, WAL
 In-memory capabilities		| yes
 User concepts				| yes (TBD)
 Storage Model				| Row-oriented On-Disk, In-Memory, RamDisk<br>Hybrid Storage (on a table-by-table basis, tables can be designated for in-memory or on-disk storage)
 Admin						| Embedded shell<br>xdb-cli tool<br>telnet<br>WEB GUI (TBD)


## Build and Install

### Download code

```bash
git clone https://github.com/crossdb-org/crossdb.git
cd crossdb
```

### Linux/MacOS/FreeBSD

```bash
make build
sudo make install
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

### CrossDB Server

https://crossdb.org/develop/server/

https://crossdb.org/admin/shell/#connect-to-crossdb-server
