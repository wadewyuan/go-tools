package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	_ "github.com/sijms/go-ora"
)

type Config struct {
	Db struct {
		Host     string
		Port     string
		Sid      string
		Username string
		Password string
	}

	Paths []string
}

//
var watcher *fsnotify.Watcher
var conf *Config

// main
func main() {
	var (
		c    string
		scan bool
	)

	// load configuration file
	flag.StringVar(&c, "c", "./config.json", "Specify the configuration file.")
	flag.BoolVar(&scan, "s", false, "Run a full scan of all the paths, then load files that are not found in database.")
	flag.Parse()
	file, err := os.Open(c)
	if err != nil {
		log.Fatal("can't open config file: ", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&conf)
	if err != nil {
		log.Fatal("can't decode config JSON: ", err)
	}

	// creates a new file watcher
	watcher, _ = fsnotify.NewWatcher()
	defer watcher.Close()

	// starting at the root of the project, walk each file/directory searching for
	// directories
	for _, p := range conf.Paths {
		if scan {
			if err := filepath.Walk(p, scanDir); err != nil {
				log.Println("ERROR", err)
			}
		} else {
			if err := filepath.Walk(p, watchDir); err != nil {
				log.Println("ERROR", err)
			} else {
				log.Printf("Watching %s\n", p)
			}
		}
	}

	if !scan {
		//
		done := make(chan bool)

		//
		go func() {
			for {
				select {
				// watch for events
				case event := <-watcher.Events:
					// log.Printf("EVENT: %s, OP: %s\n", event.Name, event.Op.String())
					if event.Op == fsnotify.CloseWrite || event.Op == fsnotify.Create {
						err := readFile(event.Name)
						if err != nil {
							log.Printf("Error processing file: %s\n ", event.Name)
						}
					}
					// watch for errors
				case err := <-watcher.Errors:
					log.Println("ERROR", err)
				}
			}
		}()

		<-done
	}
}

// watchDir gets run as a walk func, searching for directories to add watchers to
func watchDir(path string, fi os.FileInfo, err error) error {

	// since fsnotify can watch all the files in a directory, watchers only need
	// to be added to each nested directory
	if fi.Mode().IsDir() {
		return watcher.Add(path)
	}

	return nil
}

// read cdr file and get the start & end time
func readFile(path string) error {
	f, _ := os.Open(path)

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	var tBegin = time.Time{}
	var tEnd = time.Time{}

	for scanner.Scan() {
		s := strings.Split(scanner.Text(), "|")
		t, err := time.Parse("20060102150405", s[1])
		if err != nil {
			return err
		}
		if tEnd.IsZero() || t.After(tEnd) {
			tEnd = t
		}
		if tBegin.IsZero() || t.Before(tBegin) {
			tBegin = t
		}
	}
	if !tBegin.IsZero() && !tEnd.IsZero() { // Skip empty files
		logFileNameAndTimes(filepath.Base(f.Name()), tBegin, tEnd)
	}

	f.Close()
	return nil
}

func scanDir(path string, fi os.FileInfo, err error) error {
	if err != nil {
		log.Panic(err)
	}
	if fi.Mode().IsDir() {
		log.Printf("Scanning %s\n", fi.Name())
		var files []string
		err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				files = append(files, filepath.Base(info.Name()))
			}
			return nil
		})
		if err != nil {
			log.Panic(err)
		}
		// Skip processing when no files
		if len(files) == 0 {
			return nil
		}

		var s = "SELECT t.* FROM ("
		for i, file := range files {
			s += "SELECT '" + file + "' AS file_name FROM DUAL"
			if i < len(files)-1 {
				s += " UNION\n"
			}
		}
		s += ") t LEFT JOIN sms_a2p_cdr_loading_log_ext e ON e.file_name = t.file_name WHERE e.file_name IS NULL"
		// log.Print(sql)

		conn, err := sql.Open("oracle", "oracle://"+conf.Db.Username+":"+conf.Db.Password+"@"+conf.Db.Host+":"+conf.Db.Port+"/"+conf.Db.Sid)
		if err != nil {
			log.Panic(err)
		}
		defer conn.Close()

		stmt, err := conn.Prepare(s)
		if err != nil {
			log.Panic(err)
		}
		defer stmt.Close()

		rows, err := stmt.Query()
		if err != nil {
			log.Panic(err)
		}
		defer rows.Close()

		var filename string
		for rows.Next() {
			// define vars
			err = rows.Scan(&filename)
			if err != nil {
				log.Panic(err)
			}
			readFile(path + "/" + filename)
		}
	}
	return nil
}

// log the file name, begin and end times into table in oracle DB
func logFileNameAndTimes(filename string, tBegin time.Time, tEnd time.Time) {

	matched, _ := regexp.MatchString("cdr_a2pgw0[1234][abcd]_\\d{14}_\\d{3}", filename)
	if !matched {
		log.Printf("Invalid cdr file: %s\n", filename)
		return
	}

	// parse file time from filename, example: cdr_a2pgw02b_20190522093400_114
	tFile, err := time.Parse("20060102150405", filename[13:27])
	if err != nil {
		log.Printf("Failed to parse time from string: %s\n", filename[13:27])
	}

	log.Printf("File: %s, Max Time: %s, Min Time: %s", filename, tEnd.Format(time.RFC3339), tBegin.Format(time.RFC3339))
	conn, err := sql.Open("oracle", "oracle://"+conf.Db.Username+":"+conf.Db.Password+"@"+conf.Db.Host+":"+conf.Db.Port+"/"+conf.Db.Sid)
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("INSERT INTO SMS_A2P_CDR_LOADING_LOG_EXT VALUES(:1, :2, :3, :4, :5, SYSDATE)")
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	days := dateDiff(tBegin, tEnd)

	if days == 0 {
		_, err = stmt.Exec(filename, tFile, tBegin, tEnd, 1)
		if err != nil {
			log.Panic(err)
		}
	} else {
		t1 := tBegin
		t2 := endOfDate(tBegin)
		seg := 1
		for days >= 0 {
			_, err = stmt.Exec(filename, tFile, t1, t2, seg)
			if err != nil {
				log.Panic(err)
			}
			days--
			seg++
			t1 = beginningOfDate(t1.AddDate(0, 0, 1))
			t2 = endOfDate(t1)
			if t2.After(tEnd) {
				t2 = tEnd
			}
		}
	}
}

func dateDiff(t1 time.Time, t2 time.Time) int {
	rounded1 := beginningOfDate(t1)
	rounded2 := beginningOfDate(t2)
	return int(rounded2.Sub(rounded1).Hours() / 24)
}

func beginningOfDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func endOfDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 999, t.Location())
}
