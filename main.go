package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// The path to the file where the generated keys are stored.
const keysFilePath = "query_keys.json"

// QueryKey represents a primary key for a row in the test_table.
type QueryKey struct {
	EqpModel     string `json:"eqp_model"`
	StrategyName string `json:"strtgy_name"`
	JobID        string `json:"job_id"`
}

func main() {
	fmt.Println("Starting Go concurrent Cassandra query test...")

	// Get the concurrency level from the command-line argument.
	if len(os.Args) < 4 {
		log.Fatalf("Usage: go run . <concurrency_level> <number_of_queries> <keys_file_path>")
	}
	concurrency, err := strconv.Atoi(os.Args[1])
	if err != nil || concurrency <= 0 {
		log.Fatalf("Invalid concurrency level. Please provide a positive integer.")
	}
	numQueries, err := strconv.Atoi(os.Args[2])
	if err != nil || numQueries <= 0 {
		log.Fatalf("Invalid number of queries. Please provide a positive integer.")
	}
	
	keysFilePath := os.Args[3]
	

	// Read the keys from the JSON file.
	absPath, _ := filepath.Abs(keysFilePath)
	fmt.Printf("Reading query keys from %s...\n", absPath)
	file, err := os.ReadFile(keysFilePath)
	if err != nil {
		log.Fatalf("Failed to read keys file: %v", err)
	}

	var allKeys []QueryKey
	if err := json.Unmarshal(file, &allKeys); err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if len(allKeys) == 0 {
		log.Fatalf("No keys found in the JSON file. Please run the data inserter first.")
	}

	// --- Cassandra Connection Configuration ---
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "test"
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	cluster.Consistency = gocql.Quorum
	cluster.NumConns = concurrency
	cluster.Timeout = 30 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
	}
	defer session.Close()

	fmt.Println("Cassandra session established. Preparing statement...")

	// --- Query Execution ---
	// Changed from SELECT * to SELECT eqp_model to avoid the "not enough columns" error.
	query := "SELECT eqp_model FROM test_table WHERE eqp_model = ? AND job_id = ? AND strtgy_name = ?"

	fmt.Printf("Executing %d concurrent queries with a concurrency level of %d...\n", numQueries, concurrency)

	var wg sync.WaitGroup
	var successfulQueries int64

	startTime := time.Now()

	// Use a buffered channel to act as a semaphore for limiting concurrency.
	semaphore := make(chan struct{}, concurrency)

	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(queryID int) {
			defer wg.Done()
			defer func() { <-semaphore }()

			// Use a key from the pre-generated list.
			key := allKeys[queryID%len(allKeys)]

			iter := session.Query(
				query,
				key.EqpModel,
				key.JobID,
				key.StrategyName,
			).Iter()

			var dummy string
			if iter.Scan(&dummy) {
				// If Scan returns true, it means a row was found.
				successfulQueries++
			}
			
			if err := iter.Close(); err != nil {
				log.Printf("Query %d failed: %v", queryID, err)
			}
		}(i)
	}

	wg.Wait()

	totalTime := time.Since(startTime)

	fmt.Println("\nAll queries completed.")
	fmt.Printf("Total successful queries: %d\n", successfulQueries)
	fmt.Printf("Total time taken: %.2f seconds\n", totalTime.Seconds())
}
