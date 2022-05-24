package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	_ "github.com/sijms/go-ora"
	alarm "github.com/wadewyuan/smartom-utils-go"
)

// Count lines in a file, reference to https://stackoverflow.com/a/24563853
func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func main() {
	var (
		dbConn  string
		path    string
		dateStr string
	)
	flag.StringVar(&dbConn, "c", "", "Specify the oracle database connection, e.g. oracle://{username}:{password}@{host}:{port}/{sid}")
	flag.StringVar(&path, "p", ".", "The path to process")
	flag.StringVar(&dateStr, "d", time.Now().AddDate(0, 0, -1).Format("20060102"), "Determines the date of files to process, default to T-1, format is {yyyyMMdd}")
	flag.Parse()

	// Change directory
	err := os.Chdir(path)
	if err != nil {
		log.Panic(err)
	}

	// Process the files
	matches, _ := filepath.Glob("*" + dateStr + "*.CNT")
	if len(matches) != 24 {
		log.Print("Missing files on " + dateStr)
	}
	var files []string // The string array to hold file names with errors
	pattern, _ := regexp.Compile(`\d{12}_\d{3}_HKT_FIREWALL_CDR`)
	for _, match := range matches {
		log.Println("Checksum file: " + match)
		b, err := ioutil.ReadFile(match) // Read the checksum file
		if err != nil {
			log.Print(err)
		}
		c1 := strings.Split(string(b), ",")[1] // Get the count in checksum file
		dataFileName := strings.Replace(match, ".CNT", ".DAT", 1)
		file, err := os.Open(dataFileName) // Read the .DAT file and count lines
		if err != nil {
			log.Print("Error opening file: " + dataFileName)
		}
		defer file.Close()
		c2, err := lineCounter(bufio.NewReader(file))
		if err != nil {
			log.Print(err)
		}

		if c1 != fmt.Sprint(c2) {
			log.Print("Does not match data file: " + dataFileName)
			prefix := strings.Replace(match, ".CNT", "", 1)
			if pattern.Match([]byte(prefix)) {
				files = append(files, prefix)
			}
		} else {
			log.Print("Matches data file: " + dataFileName)
		}
	}

	// Delete the records from boss.settlem_reported_data_header so they can be regenerated
	if len(files) > 0 {
		s := "DELETE FROM boss.settlem_reported_data_header WHERE"
		for i, f := range files {
			s += " report_file_name LIKE '" + f + "%'"
			if i < len(files)-1 {
				s += " OR"
			}
		}
		log.Print(s)
		var msg = "Variances found for files: " + strings.Join(files, ",") + "<br>The tool will auto-fix them, please double check"
		alarm.SendAlarm("401-10", msg)
		conn, err := sql.Open("oracle", dbConn)
		if err != nil {
			log.Panic(err)
		}
		defer conn.Close()

		stmt, err := conn.Prepare(s)
		if err != nil {
			log.Panic(err)
		}
		defer stmt.Close()

		res, err := stmt.Exec()
		if err != nil {
			log.Panic(err)
		}
		deleted, err := res.RowsAffected()
		if err != nil {
			log.Panic(err)
		}
		log.Printf("Deleted records: %d", deleted)
	} else {
		alarm.SendAlarm("401-10", "No variances for "+dateStr)
	}
}
