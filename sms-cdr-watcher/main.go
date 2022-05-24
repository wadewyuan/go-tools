package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
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

var A2PGW_FILENAME = "cdr_a2pgw0[0-9][a-z]_\\d{14}_\\d{3}"             // cdr_a2pgw03a_20220117161852_195
var CLASS_40_FILENAME = "CDR_P2PGW0[0-9][A-Z]_IP\\d{2}_\\d{14}_\\d{3}" // CDR_P2PGW03A_IP37_20220108132921_161
var TS_FILENAME = "TS\\d{2}_\\d{14}_\\d{2}\\.pp"                       // TS04_20211220171812_19.pp
var MMX_FILENAME = "MMX_\\d{14}_\\d\\.csv"                             // MMX_20211220155600_1.csv
var HUB_FILENAME = "CDR_smshub\\d{2}_\\d{14}_\\d{3}"                   // CDR_smshub05_20211221091606_111
var SMSC_FILENAME = "cdr_\\d{2}_smsc\\d{2}[abcd]_\\d{14}_\\d{3}"       // cdr_00_smsc10a_20211225085228_106

var DATETIME_LAYOUT1 = "20060102150405"
var DATETIME_LAYOUT2 = "060102150405008"

func matches(s string, pattern string) bool {
	matched, _ := regexp.MatchString(pattern, s)
	return matched
}

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

	var separator string
	var deliveryTimeIdx int
	var dateTimePattern string
	var gwType int

	if matches(path, A2PGW_FILENAME) {
		separator = "|"
		deliveryTimeIdx = 1
		dateTimePattern = DATETIME_LAYOUT1
		gwType = 1
	} else if matches(path, CLASS_40_FILENAME) {
		separator = "|"
		deliveryTimeIdx = 22
		dateTimePattern = DATETIME_LAYOUT1
		gwType = 0
	} else if matches(path, TS_FILENAME) {
		separator = "C"
		deliveryTimeIdx = 1
		dateTimePattern = DATETIME_LAYOUT2
		gwType = 0
	} else if matches(path, MMX_FILENAME) {
		separator = "C"
		deliveryTimeIdx = 1
		dateTimePattern = DATETIME_LAYOUT2
		gwType = 0
	} else if matches(path, HUB_FILENAME) {
		separator = "|"
		deliveryTimeIdx = 22
		dateTimePattern = DATETIME_LAYOUT1
		gwType = 0
	} else if matches(path, SMSC_FILENAME) {
		separator = "C"
		deliveryTimeIdx = 1
		dateTimePattern = DATETIME_LAYOUT2
		gwType = 0
	} else {
		return errors.New("invalid cdr filename: " + path)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if dateTimePattern == DATETIME_LAYOUT2 && len(line) > 140 {
			line = line[100:140] // Get the time string by fixed length
		}
		s := strings.Split(line, separator)
		if len(s) <= deliveryTimeIdx {
			continue
		}

		t, err := time.Parse(dateTimePattern, s[deliveryTimeIdx])
		if err != nil {
			continue
		}
		if tEnd.IsZero() || t.After(tEnd) {
			tEnd = t
		}
		if tBegin.IsZero() || t.Before(tBegin) {
			tBegin = t
		}
	}
	if !tBegin.IsZero() && !tEnd.IsZero() { // Skip empty files
		err := logFileNameAndTimes(gwType, filepath.Base(f.Name()), tBegin, tEnd)
		if err != nil {
			return err
		}
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

		for _, f := range files {
			err = readFile(path + "/" + f)
			if err != nil {
				log.Printf("Error processing file: %s\n ", path+"/"+f)
			}
		}
	}
	return nil
}

// log the file name, begin and end times into table in oracle DB
func logFileNameAndTimes(gwType int, filename string, tBegin time.Time, tEnd time.Time) error {
	log.Printf("Gateway Type: %d, File: %s, Max Time: %s, Min Time: %s", gwType, filename, tEnd.Format(time.RFC3339), tBegin.Format(time.RFC3339))

	conn, err := sql.Open("oracle", "oracle://"+conf.Db.Username+":"+conf.Db.Password+"@"+conf.Db.Host+":"+conf.Db.Port+"/"+conf.Db.Sid)
	if err != nil {
		return err
	}
	defer conn.Close()

	stmt, err := conn.Prepare("INSERT INTO SMS_CDR_LOADING_LOG_EXT VALUES(sms_cdr_loading_log_seq.nextval, :1, :2, :3, :4, SYSDATE, null)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(gwType, filename, tBegin, tEnd)
	if err != nil {
		return err
	}
	return nil
}
