package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"
	"github.com/crossdb-org/crossdb/connector/go"
)

const (
	numUsers        = 1000000
	numBenchmarkRuns = 1000
)

var firstNames = []string{"John", "Jane", "Michael", "Emily", "David", "Sarah", "Christopher", "Jessica", "Matthew", "Jennifer"}
var lastNames = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}

func generateRandomName() string {
	return firstNames[rand.Intn(len(firstNames))] + " " + lastNames[rand.Intn(len(lastNames))]
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Open a connection to CrossDB
	conn, err := crossdb.Open("./benchmark_db")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Create the users table
	_, err = conn.Exec("DROP TABLE IF EXISTS users")
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.Exec("CREATE TABLE users (id INT PRIMARY KEY, name CHAR(50), age INT)")
	if err != nil {
		log.Fatal(err)
	}

	// Insert 1,000,000 users
	fmt.Println("Inserting 1,000,000 users...")
	startTime := time.Now()
	for i := 1; i <= numUsers; i++ {
		name := generateRandomName()
		age := rand.Intn(50) + 20 // Random age between 20 and 69
		_, err := conn.Exec(fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, '%s', %d)", i, name, age))
		if err != nil {
			log.Printf("Error inserting user %d: %v", i, err)
		}
		if i%100000 == 0 {
			fmt.Printf("%d users inserted...\n", i)
		}
	}
	fmt.Printf("Insertion completed in %v\n", time.Since(startTime))

	// Benchmark random SELECT with LIKE queries
	fmt.Println("\nRunning benchmark...")
	totalDuration := time.Duration(0)
	for i := 0; i < numBenchmarkRuns; i++ {
		searchName := firstNames[rand.Intn(len(firstNames))]
		query := fmt.Sprintf("SELECT * FROM users WHERE name LIKE '%%%s%%'", searchName)
		
		startTime := time.Now()
		res, err := conn.Exec(query)
		if err != nil {
			log.Printf("Error executing query: %v", err)
			continue
		}
		duration := time.Since(startTime)
		totalDuration += duration

		// Count the results
		count := 0
		for res.FetchRow() != nil {
			count++
		}
		
		fmt.Printf("Run %d: Query '%s' returned %d results in %v\n", i+1, query, count, duration)
	}

	averageDuration := totalDuration / numBenchmarkRuns
	fmt.Printf("\nBenchmark completed.\n")
	fmt.Printf("Average query time: %v\n", averageDuration)
}
