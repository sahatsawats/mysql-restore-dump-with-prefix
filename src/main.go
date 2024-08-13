package main

import (
	_ "database/sql"
	"fmt"
	"log"
	"os"
	_ "os/exec"
	"path/filepath"
	"strings"
	_ "sync"
	"time"
	"io"
	"github.com/sahatsawats/mysql-restore-dump-with-prefix/src/models"
	"gopkg.in/yaml.v2"
	"github.com/sahatsawats/concurrent-queue"
)

// TODO: Make the directory if not exists
func makeDirectory(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.Mkdir(path, 0755)

		if err != nil {
			return err
		}
	}

	return nil
}

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
		log.Fatalf("error opening file: %v", err)
	}
	defer Logfile.Close()
	// Setup the logger to show on stdout as well as appending to log file
	mw := io.MultiWriter(os.Stdout, Logfile)
	// Setup logger properties
	log.SetOutput(mw)
	log.SetFlags(log.LstdFlags)

	programUsageTime := time.Since(programStartTime)
	log.Println(programUsageTime)
}
