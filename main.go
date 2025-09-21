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

	fmt.Printf("Executing %d concurrent queries with a concurrency level of %d...\n", numQueries, concurrency)

	var wg sync.WaitGroup
	var successfulQueries int64
	var mu sync.Mutex

	startTime := time.Now()

	// Create a channel to send jobs (query IDs) to workers
	jobs := make(chan int, numQueries)

	// Start a fixed number of worker goroutines
	for w := 1; w <= concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for queryID := range jobs {
				key := allKeys[queryID%len(allKeys)]
		
				iter := session.Query(
					"SELECT eqp_model FROM test_table WHERE eqp_model = ? AND job_id = ? AND strtgy_name = ?",
					key.EqpModel, key.JobID, key.StrategyName,
				).Iter()
		
				var dummy string
				if iter.Scan(&dummy) {
					mu.Lock()
					successfulQueries++
					mu.Unlock()
				}
		
				if err := iter.Close(); err != nil {
					log.Printf("Query %d failed: %v", queryID, err)
				}
			}
		}(w)
	}
	
	// Submit all the jobs to the channel
	for i := 0; i < numQueries; i++ {
		jobs <- i
	}
	close(jobs)

	// Wait for all workers to complete their jobs
	wg.Wait()

	totalTime := time.Since(startTime)

	fmt.Println("\nAll queries completed.")
	fmt.Printf("Total successful queries: %d\n", successfulQueries)
	fmt.Printf("Total time taken: %.2f seconds\n", totalTime.Seconds())
}
