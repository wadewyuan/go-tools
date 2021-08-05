package main

import (
	"bufio"
	"database/sql"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/iafan/cwalk"
	gzip "github.com/klauspost/pgzip"
	_ "github.com/sijms/go-ora"
)

func isValidForGzip(info os.FileInfo, pattern string) bool {
	filename := info.Name()
	// some files' year-month format is YYYY-MM
	pattern2 := pattern[:4] + "-" + pattern[4:6]
	return !info.IsDir() && !strings.HasSuffix(filename, ".gz") && !strings.HasSuffix(filename, ".zip") && !strings.HasSuffix(filename, ".Z") &&
		(strings.Contains(filename, pattern) || strings.Contains(filename, pattern2))
}

func gzipFiles(workPath, monthPattern string) {
	err := os.Chdir(workPath)
	if err != nil {
		log.Printf("Error changing directory: %s. Skipping \n", workPath)
		return
	}
	if _, err := os.Stat(monthPattern); os.IsNotExist(err) {
		os.Mkdir(monthPattern, 0755)
	}
	archivePath := workPath + string(os.PathSeparator) + monthPattern
	err = cwalk.Walk(workPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !isValidForGzip(info, monthPattern) {
				return nil
			}
			log.Printf("Gzipping file %s\n", info.Name())
			f, _ := os.Open(path)

			reader := bufio.NewReader(f)
			content, _ := ioutil.ReadAll(reader)
			// Add gz extension and move to the archive directory
			zippedFilePath := archivePath + string(os.PathSeparator) + info.Name() + ".gz"
			// Open file for writing.
			f, _ = os.Create(zippedFilePath)
			// Write compressed data.
			w := gzip.NewWriter(f)
			w.Write(content)
			w.Close()

			// Remove the original file
			os.Remove(path)
			return nil
		})
	if err != nil {
		log.Print(err)
	}
}

func main() {
	var (
		basePath     string
		monthPattern string
		dbConnection string
	)
	flag.StringVar(&basePath, "path", ".", "The root path to recursively walk and gzip files")
	flag.StringVar(&monthPattern, "month", time.Now().AddDate(0, -2, 0).Format("200601"), "Specify the files containing the month string with yyyyMM format")
	flag.StringVar(&dbConnection, "db-conn", "", "[Optional] If not specified, the program would process the \"path\" flag. Oracle db string to access the config table smsprd.sms_opt_ftp. e.g. oracle://{username}:{password}@{host}:{port}/{sid}")

	flag.Parse()

	log.Printf("Working Path : %s\n", basePath)
	log.Printf("Month Pattern: %s\n", monthPattern)
	log.Printf("DB Connection: %s\n", dbConnection)

	if len(dbConnection) <= 0 {
		// If the db connection was not passed in, recursively process the base path
		gzipFiles(basePath, monthPattern)
	} else {
		// Process each FTP user's home directory based on the settings in table smsprd.sms_opt_ftp
		conn, err := sql.Open("oracle", dbConnection)
		if err != nil {
			log.Panic(err)
		}
		defer conn.Close()

		stmt, err := conn.Prepare("SELECT operator, homedir FROM smsprd.sms_opt_ftp WHERE eff_time <= :1 AND exp_time >= :2")
		if err != nil {
			log.Panic(err)
		}
		defer stmt.Close()

		var (
			operator string
			homeDir  string
		)
		rows, err := stmt.Query(time.Now(), time.Now())
		if err != nil {
			log.Panic(err)
		}

		var paths []string
		for rows.Next() {
			// traverse the resultset and gzip files
			err = rows.Scan(&operator, &homeDir)
			if err != nil {
				log.Panic(err)
			}
			// Gzipping may take a long time and cause rows.Next() panic, so we cache the results in a slice then process it outside this loop
			// gzipFiles(basePath+string(os.PathSeparator)+homeDir, monthPattern)
			paths = append(paths, basePath+string(os.PathSeparator)+homeDir)
		}
		err = rows.Err()
		if err != nil {
			log.Panic(err)
		}
		rows.Close()
		for _, p := range paths {
			gzipFiles(p, monthPattern)
		}

	}

}
