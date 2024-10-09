package main

import (
        "fmt"
        "log"
        "github.com/crossdb-org/crossdb/go"
)

func main() {
        // Open a connection to CrossDB
	conn, err := crossdb.Open(":memory:")
        if err != nil {
                log.Fatal(err)
        }
        defer conn.Close()

        /*
        // Drop the table if it exists
        _, err = conn.Exec("DROP TABLE IF EXISTS users")
        if err != nil {
                log.Fatal(err)
        }
        */

        // Create a table
        _, err = conn.Exec("CREATE TABLE users (id INT PRIMARY KEY, name CHAR(50), age INT)")
        if err != nil {
                log.Fatal(err)
        }

        // Insert some data
        _, err = conn.Exec("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")
        if err != nil {
                log.Fatal(err)
        }

        // Query the data
        res, err := conn.Exec("SELECT * FROM users")
        if err != nil {
                log.Fatal(err)
        }

        // Print the results
        fmt.Println("All users:")
        for row := res.FetchRow(); row != nil; row = res.FetchRow() {
                fmt.Printf("ID: %d, Name: %s, Age: %d\n", row[0].(int), row[1].(string), row[2].(int))
        }

        // Update a record
        _, err = conn.Exec("UPDATE users SET age = 31 WHERE id = 1")
        if err != nil {
                log.Fatal(err)
        }

        // Query a specific user
        res, err = conn.Exec("SELECT * FROM users WHERE id = 1")
        if err != nil {
                log.Fatal(err)
        }

        fmt.Println("\nUpdated user:")
        if row := res.FetchRow(); row != nil {
                fmt.Printf("ID: %d, Name: %s, Age: %d\n", row[0].(int), row[1].(string), row[2].(int))
        }

        // Delete a record
        _, err = conn.Exec("DELETE FROM users WHERE id = 2")
        if err != nil {
                log.Fatal(err)
        }

        // Query all users again
        res, err = conn.Exec("SELECT * FROM users")
        if err != nil {
                log.Fatal(err)
        }

        fmt.Println("\nRemaining users:")
        for row := res.FetchRow(); row != nil; row = res.FetchRow() {
                fmt.Printf("ID: %d, Name: %s, Age: %d\n", row[0].(int), row[1].(string), row[2].(int))
        }

        // Demonstrate transaction
        err = conn.Begin()
        if err != nil {
                log.Fatal(err)
        }

        _, err = conn.Exec("INSERT INTO users (id, name, age) VALUES (4, 'David', 40)")
        if err != nil {
                conn.Rollback()
                log.Fatal(err)
        }

        err = conn.Commit()
        if err != nil {
                log.Fatal(err)
        }

        // Verify the transaction
        res, err = conn.Exec("SELECT * FROM users WHERE id = 4")
        if err != nil {
                log.Fatal(err)
        }

        fmt.Println("\nUser added in transaction:")
        if row := res.FetchRow(); row != nil {
                fmt.Printf("ID: %d, Name: %s, Age: %d\n", row[0].(int), row[1].(string), row[2].(int))
        }

        fmt.Printf("\nCrossDB Version: %s\n", crossdb.Version())
}
