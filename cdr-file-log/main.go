package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/iafan/cwalk"
)

func process(workPath string) {
	err := os.Chdir(workPath)
	if err != nil {
		log.Printf("Error changing directory: %s. Skipping \n", workPath)
		return
	}

	err = cwalk.Walk(workPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			log.Printf("Reading file %s\n", info.Name())
			f, _ := os.Open(path)

			scanner := bufio.NewScanner(f)
			scanner.Split(bufio.ScanLines)
			var tMax = time.Time{}
			var tMin = time.Time{}

			for scanner.Scan() {
				s := strings.Split(scanner.Text(), "|")
				t, err := time.Parse("20060102150405", s[1])
				if err != nil {
					log.Printf("Failed to parse time from string: %s\n", s[1])
				}
				if tMax.IsZero() || t.After(tMax) {
					tMax = t
				}
				if tMin.IsZero() || t.Before(tMin) {
					tMin = t
				}
			}
			log.Printf("File: %s, Max Time: %s, Min Time: %s", info.Name(), tMax.Format(time.RFC3339), tMin.Format(time.RFC3339))

			f.Close()

			return nil
		})
	if err != nil {
		log.Panic(err)
	}
}

func main() {
	var (
		basePath     string
		pattern      string
		dbConnection string
	)

	flag.StringVar(&basePath, "path", ".", "The root path to walk and process files")
	// flag.StringVar(&pattern, "month", time.Now().AddDate(0, -2, 0).Format("200601"), "Specify the files containing the month string with yyyyMM format")
	flag.StringVar(&dbConnection, "db-conn", "", "[Optional] If not specified, the program would process the \"path\" flag. Oracle db string to access the config table smsprd.sms_opt_ftp. e.g. oracle://{username}:{password}@{host}:{port}/{sid}")

	flag.Parse()

	log.Printf("Working Path : %s\n", basePath)
	log.Printf("Pattern: %s\n", pattern)
	log.Printf("DB Connection: %s\n", dbConnection)

	process(basePath)
}
