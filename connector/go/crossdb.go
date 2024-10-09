package crossdb

// #cgo CFLAGS: -I../src
// #cgo LDFLAGS: -L. -lcrossdb
// #include <crossdb.h>
// #include <stdlib.h>
import "C"
import (
        "errors"
        "fmt"
        "unsafe"
)

type Conn struct {
        conn *C.xdb_conn_t
}

type Result struct {
        res *C.xdb_res_t
}

// Open opens a new CrossDB connection
func Open(dbPath string) (*Conn, error) {
        cPath := C.CString(dbPath)
        defer C.free(unsafe.Pointer(cPath))

        conn := C.xdb_open(cPath)
        if conn == nil {
                return nil, errors.New("failed to open database")
        }

        return &Conn{conn: conn}, nil
}

// Close closes the CrossDB connection
func (c *Conn) Close() {
        C.xdb_close(c.conn)
}

// Exec executes an SQL statement
func (c *Conn) Exec(sql string) (*Result, error) {
        cSQL := C.CString(sql)
        defer C.free(unsafe.Pointer(cSQL))

        res := C.xdb_exec(c.conn, cSQL)
        if res == nil {
                return nil, errors.New("failed to execute SQL")
        }

        if res.errcode != 0 {
                errMsg := C.GoString(C.xdb_errmsg(res))
                defer C.xdb_free_result(res)
                return nil, fmt.Errorf("SQL error %d: %s", res.errcode, errMsg)
        }

        return &Result{res: res}, nil
}

// FetchRow fetches a row from the result set
func (r *Result) FetchRow() []interface{} {
        row := C.xdb_fetch_row(r.res)
        if row == nil {
                return nil
        }

        colCount := int(r.res.col_count)
        result := make([]interface{}, colCount)

        for i := 0; i < colCount; i++ {
                col := C.xdb_column_meta(C.uint64_t(r.res.col_meta), C.uint16_t(i))
                switch col.col_type {
                case C.XDB_TYPE_INT:
                        result[i] = int(C.xdb_column_int(C.uint64_t(r.res.col_meta), row, C.uint16_t(i)))
                case C.XDB_TYPE_BIGINT:
                        result[i] = int64(C.xdb_column_int64(C.uint64_t(r.res.col_meta), row, C.uint16_t(i)))
                case C.XDB_TYPE_FLOAT:
                        result[i] = float32(C.xdb_column_float(C.uint64_t(r.res.col_meta), row, C.uint16_t(i)))
                case C.XDB_TYPE_DOUBLE:
                        result[i] = float64(C.xdb_column_double(C.uint64_t(r.res.col_meta), row, C.uint16_t(i)))
                case C.XDB_TYPE_CHAR:
                        result[i] = C.GoString(C.xdb_column_str(C.uint64_t(r.res.col_meta), row, C.uint16_t(i)))
                default:
                        result[i] = nil
                }
        }

        return result
}

// FreeResult frees the result set
func (r *Result) FreeResult() {
        C.xdb_free_result(r.res)
}

// Begin starts a new transaction
func (c *Conn) Begin() error {
        ret := C.xdb_begin(c.conn)
        if ret != 0 {
                return errors.New("failed to begin transaction")
        }
        return nil
}

// Commit commits the current transaction
func (c *Conn) Commit() error {
        ret := C.xdb_commit(c.conn)
        if ret != 0 {
                return errors.New("failed to commit transaction")
        }
        return nil
}

// Rollback rolls back the current transaction
func (c *Conn) Rollback() error {
        ret := C.xdb_rollback(c.conn)
        if ret != 0 {
                return errors.New("failed to rollback transaction")
        }
        return nil
}

// Version returns the CrossDB version
func Version() string {
        return C.GoString(C.xdb_version())
}