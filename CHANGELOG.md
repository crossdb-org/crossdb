# Change Log

<!--
- distinct
- group by, having
- select expr a+b, a*b
- insert expr
- where a<b
- where exp: OR
- join
- operators: like, in, between
- data types: BOOL, TIMESTAMP
- function (math[abs,round,floor], str[length], time[now()])
- client-server mode
- NULL, autoincr ID
- WAL
- TCP-B
- TCP-C

## 0.x.0 <small>(2024-08-26)</small>

**Features**
**Improvements**
**Test**
**Bug Fixes**
- Support simple SQL inner join
- Support new statements: `FLUSH` `REPAIR`

- Support RBTREE index for exact match, range match and leftmost match

-->

## 0.13.0 <small>(2025-05-07)</small>

**Features**

- Support `REPLACE` statement
- Support `Trigger` feature
- Support `Replication` feature

**Improvements**

**Bug Fixes**

- Can't open existing database
- Crash when there's query error
- `order by` doesn't work for `VARCHAR`/`VARBINARY`
- `xdb_bexec` doesn't work for `VARCHAR`/`VARBINARY`
- `inet` type query doesn't work
- Python client crash

**Test**

## 0.12.0 <small>(2025-04-14)</small>

**Features**

- Support data types: `INET`(IPv4 and IPv6 host and subnet), `MAC` address
- Support SQL statements: `SHOW CREATE DATABASE`
- Support Update SET value from NULL to valid or valid to NULL
- Support embedded SOURCE sql file
- Add many new APIs and change many APIs prototypes

**Improvements**

**Test**

**Bug Fixes**

- Hash index flush doesn't do flush action
- Fix BOOL type bugs


## 0.11.0 <small>(2024-11-11)</small>

**Features**

- Support data types: `BOOL`, `TIMESTAMP`
- Support SQL statements: `CREATE SERVER` `DROP SERVER` `SHOW SERVERS`
- Support APIs: `xdb_connect`
- Support embedded SERVER mode
- Support `xdb-cli` standalone server mode
- Support telnet connection

**Improvements**

**Test**

**Bug Fixes**

- INSERT without column list will set all columns to NULL
- WAL flush with wrong address and range
- Crash when table drop during flush


## 0.10.0 <small>(2024-11-01)</small>

**Features**

- Support `VARCHAR`, `BINARY`, `VARBINARY`
- Support `WAL` [#31](https://github.com/crossdb-org/crossdb/issues/31)
- Support `OR` operator
- Support new API `xdb_column_blob`
- Support `CREATE DATABASE` `SYNCMODE` option

**Improvements**

**Test**

**Bug Fixes**

- Create duplicate database doesn't report error sometimes


## 0.9.0 <small>(2024-10-11)</small>

**Features**

- Support `LIKE` operator
- Support `NOLOCK` mode
- Add new APIs: `xdb_column_type` `xdb_column_name` `xdb_type2str` 
- Add `GO` `Python` `C++` drivers

**Improvements**

- Optimize field lookup

**Test**

- Optimize `STL Map` and `HashMap(unordered_map)` benchmark test driver
- Add `Boost` `MultiIndex` benchmark test driver
- Add more smoke test cases

**Bug Fixes**

- The index was incorrectly created as the primary key
- Update many rows with expression will set all rows to first row's value
- Auto-completion heap buffer access overflow
- Update transaction heap use after free
- Wrong Aggregation function result for empty table
- Update transaction crash issue
- SQL syntax error


## 0.8.0 <small>(2024-09-03)</small>

**Features**

- `SELECT` supports simple expr, ex: `a + 10` `a - b` [#12](https://github.com/crossdb-org/crossdb/issues/12)
- `SELECT` supports `AS`
- Support operators: `>`, `>=`, `<`, `<=`, `!=`, `<>` [#13](https://github.com/crossdb-org/CrossDB/issues/13)
- `WHERE` expression supports having the field on the right side, ex: `5 < id`
- `xdb-cli` creates a default memory database, allowing you to directly create tables for practice.
- Support `CMake`

**Improvements**

- Optimize `INSERT` `UPDATE` `DELETE` auto-commit performance for in-memory database 

**Test**

- Refactor benchmark test framework and support binding cpu core
- New crossdb and `SQLite` benchmark test driver
- Add C++ `STL Map` and `HashMap(unordered_map)` benchmark test driver
- Import unit test framework
- Add few test cases

**Bug Fixes**

- Fix bug [#15](https://github.com/crossdb-org/CrossDB/issues/15) Segmentation fault occurs while using on-disk database
- Fix close connection then reopen connection will cause previous connection use-after-free issue and memory leak issue
- Fix query misses issue during hash index rehashing


## 0.7.0 <small>(2024-08-26)</small>

**Features**

- `UPDATE` SET clause supports simple expression, ex: `SET val=val+5` `SET val=a-b`
- `UPDATE` SET supports prepared statement
- `INSERT` supports prepared statement
- New APIs: `xdb_bexec`, `xdb_vbexec`, `xdb_stmt_bexec`, `xdb_stmt_vexec`, `xdb_clear_bindings`

**Improvements**

- `INSERT` parser avoids malloc
- `UPDATE` only updates affected indexes
- Optimize `INSERT` `UPDATE` `DELETE` auto-commit performance for in-memory database 

**Test**

- Improve benchmark test
- Add `SQLite` benchmark test

**Bug Fixes**

- Fix hash index infinite loop issue
- Fix bench test time unit `ns` to `us`


## 0.6.0 <small>(2024-08-18)</small>

**Initial refactor release**

- This project was redesigned and rewritten from scratch for over one year
- Standard RDBMS model
- New SQL APIs which can support more language
- MySQL style SQL and shell, which will be easy to study.


## 0.5.0 <small>(2023-06-26)</small>

**Features**

- CrossDB command line tool `crossdb-cli` is released
- Optimize insert/update/query/delete performance
- Add new API `cross_matchCreate` and `cross_matchFree`
- DML APIs supports `cross_fields_h` and `cross_match_h`

**Bug Fixes**


## 0.4.0 <small>(2023-06-20)</small>

**Features**

- Support FreeBSD(X64)
- Optimize insert/update/query/delete performance
- Add new API `cross_fieldsCreate` and `cross_fieldsFree`

**Bug Fixes**


## 0.3.0 <small>(2023-06-13)</small>

**Features**

- Support MacOS (X64 and ARM64)
- Change `CROSS_DB_XXX` to `CROSS_XXX`

**Bug Fixes**

- `cross_dbTblCreate` flags `CROSS_DB_RBTREE` doesn't create Primary Key Index type correctly


## 0.2.0 <small>(2023-06-07)</small>

**Features**

- Support Windows
- Support Linux ARM64

**Bug Fixes**


## 0.1.0 <small>(2023-06-03)</small>

- **Initial release**
