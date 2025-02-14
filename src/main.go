package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sahatsawats/concurrent-queue"
	"github.com/sahatsawats/mysql-restore-dump-with-prefix/src/models"
	"gopkg.in/yaml.v2"
)

// TODO: Reading configuration file from ./conf/config.yaml based on executable path
func readingConfigurationFile() *models.Configurations {
	// get the current execute directory
	baseDir, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}

	// Join path to config file
	configFile := filepath.Join(filepath.Dir(baseDir), "conf", "config.yaml")
	// Read file in bytes for mapping yaml to structure with yaml package
	readConf, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to read configuration file: %v", err)
	}

	// Map variable to configuration function
	var conf models.Configurations
	// Map yaml file to config structure
	err = yaml.Unmarshal(readConf, &conf)
	if err != nil {
		log.Fatalf("Failed to unmarshal config file: %v", err)
	}

	return &conf
}

func commaSplit(str string) []string {
	return strings.Split(str, ",")
}

func restoreDumpFile(id int, wg *sync.WaitGroup, databaseCredentails *models.DatabaseCrednetials, jobQueue *concurrentqueue.ConcurrentQueue[models.JobQueue], repairQueue *concurrentqueue.ConcurrentQueue[models.JobQueue], prefix string, isRepair bool) {
	var nok int
	defer wg.Done()
	for {
		// Define break condition, if queue is empty -> exit
		if jobQueue.IsEmpty() {
			break
		}

		// Dequeue dump name from concurrent queue
		dumpInfo := jobQueue.Dequeue()

		// Normally, the dump dir came with "<database-name>-staging"
		// Therefore, remove the extension for retrieved actual database name
		rawSchemaName := strings.ReplaceAll(dumpInfo.DirName, "-staging", "")
		// Add the prefix infront of database name
		prefixSchemaName := fmt.Sprintf("%s%s", prefix, rawSchemaName)

		// Bash execution:
		// mysqlsh -h <host> -P <port> -u <user> -p<password> --js -e "util.loadDump('path-to-dir', {schema: 'database_name', threads: 4})"
		cmd := exec.Command(
			"mysqlsh", "-h", databaseCredentails.DBAddress, "-P", databaseCredentails.DBPort, "-u", databaseCredentails.User,
			fmt.Sprintf("-p%s", databaseCredentails.Password),
			"--js",
			"-e", fmt.Sprintf("util.loadDump('%s', {schema: '%s',threads: 4})",dumpInfo.FullPath, prefixSchemaName),
		)

		// execute the cmd
		err := cmd.Run()
		if err != nil {
			// Enqueue database to reqair queue if is not in repair process.
			if !isRepair {
				log.Printf("[WARNING] Thread%d: error from restore database name %s from path %s: %v \n", id, prefixSchemaName, dumpInfo.FullPath, err)
				nok += 1
				log.Printf("[INFO] Enqueue %s to repair queue. \n", rawSchemaName)
				repairQueue.Enqueue(dumpInfo)
			} else {
				nok += 1
				log.Printf("[ERROR] Repair Service: Thread%d: failed to restore %s to destination MySQL servers: %v", id, rawSchemaName, err)
			}
		} 
	}
	log.Printf("[INFO] Thread%d: Complete restore database to MySQL with error report: %d", id, nok)
}

func main() {
	programStartTime := time.Now()
	fmt.Println("Start reading configuration file...")
	config := readingConfigurationFile()
	fmt.Println("Complete reading configuration file.")

	// Join logging path
	logFilePath := filepath.Join(config.Logger.LOG_DIRECTORY, config.Logger.LOG_FILENAME)
	// Open log path
	Logfile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("[ERROR] Error opening file: %v", err)
	}
	defer Logfile.Close()
	// Setup the logger to show on stdout as well as appending to log file
	mw := io.MultiWriter(os.Stdout, Logfile)
	// Setup logger properties
	log.SetOutput(mw)
	log.SetFlags(log.LstdFlags)

	// Create a concurrent queue for filename and retrying file name
	jobQueue := concurrentqueue.New[models.JobQueue]()
	retryQueue := concurrentqueue.New[models.JobQueue]()

	// Define database credentials
	databaseCredentails := &models.DatabaseCrednetials{
		DBAddress: config.Server.ADDRESS,
		DBPort: fmt.Sprintf("%d",config.Server.PORT),
		User: config.Database.DB_USER,
		Password: config.Database.DB_PASSWORD,
	}

	// Split the directory from configuration
	listOfDumpDirectory := commaSplit(config.Software.DUMP_FILE_DIRECTORYS)
	log.Println("[INFO] Complete reading the dump directory. Total directory:", len(listOfDumpDirectory))
	log.Println("[INFO] Start enqueue file in directory...")
	// Loop through provided directory which each directory hold dump directorys
	for _, rootDir := range listOfDumpDirectory{
		// Reading all directory in rootDir, if error -> exit with status 1
		listOfDirInfo, err := os.ReadDir(rootDir)
		if err != nil {
			log.Fatalf("[ERROR] Failed to reading directory %s with error: %v", rootDir, err)
		}
		// Loop through each directory in rootDir, filter out other file format.
		for _, dirInfo := range listOfDirInfo {
			if dirInfo.IsDir() {
				// create job structure that contains DirName (basedir) and FullPath to that directory
				job := models.JobQueue{
					DirName: dirInfo.Name(),
					FullPath: filepath.Join(rootDir,dirInfo.Name()),
				}
				// Enqueue the job structure
				jobQueue.Enqueue(job)
			}
			// If not directory, skip.
		}
		log.Println("[INFO] Complete enqueue all directory in", rootDir)
	}

	// Define wait group for each concurrent
	var wg sync.WaitGroup
	// for loop add concurrent goroutine to wait group.
	restoreThreads := config.Software.RESTORE_THREADS
	for i := 1; i <= restoreThreads; i++ {
		wg.Add(1)
		restoreDumpFile(i, &wg, databaseCredentails, jobQueue, retryQueue, config.Software.DESTINATION_PREFIX, false)
	}

	// Waiting for goroutine process to be complete
	wg.Wait()

	// Check condition that retry queue is empty or not. If not -> repair
	if !(retryQueue.IsEmpty()) {
		log.Println("[INFO] Repair Service: Initiated. Number of repair requests found. Starting the repair process.")
		// Reference same amount of threads
		for i := 1; i <= restoreThreads; i++ {
			wg.Add(1)
			// Reuse same function, change jobQueue to retryQueue
			restoreDumpFile(i, &wg, databaseCredentails, retryQueue, nil, config.Software.DESTINATION_PREFIX, true)
		}
		// Wait for concurrent threads to be complate
		wg.Wait()
		log.Println("[INFO] Complete repair process.")
	}
	
	programUsageTime := time.Since(programStartTime)
	log.Println("[INFO] Complete restore the process with time usages:", programUsageTime)
}
