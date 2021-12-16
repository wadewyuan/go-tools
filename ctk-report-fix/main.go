package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type empty struct{}

type result struct {
	Filename   string
	Line       string
	LineNumber int
	Error      error
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func processFile(path string, p string) (filename string, err error) {
	log.Println("Updating " + path)
	// Step 1. Extract the report type and date from file path, e.g. CTK_IP_so_SM_ALL_202105280000
	// regexp.MustCompile(`(?P<Year>\d{4})-(?P<Month>\d{2})-(?P<Day>\d{2})`)
	re := regexp.MustCompile(`.*([s|f|a][i|o]_[SD]).*_(\d{8}?).*`)
	match := re.FindStringSubmatch(path)

	// Step 2. Concatenate date string to the pattern to narrow down the search range
	p = "*" + match[1] + p + match[2] + "*"

	// Step 3. Open the file to update and read line by line
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()

	newFile, err := os.Create(path + ".new")
	check(err)
	defer newFile.Close()
	// Create a writer
	w := bufio.NewWriter(newFile)

	scanner := bufio.NewScanner(f)
	count := 0
	var wg sync.WaitGroup
	for scanner.Scan() {
		// Step 4. Replace "OTHER" in each line with ".*" for searching
		line := scanner.Text()
		if strings.HasPrefix(line, "There") || len(line) < 1 {
			// Skip empty lines and the last line of summary
			continue
		}
		str := strings.Replace(line, "OTHER", ".*", -1)
		str = strings.Replace(str, "?", "\\?", -1)
		pat, err := regexp.Compile(str)
		check(err)
		// Step 5. Grep the pattern in files
		files, err := filepath.Glob(p)
		check(err)
		chResults := make(chan *result)

		wg.Add(len(files))
		for _, sPath := range files {
			go grep(sPath, pat, &wg, chResults)
		}
		go func(wait *sync.WaitGroup, ch chan<- *result) {
			wg.Wait()
			close(ch)
		}(&wg, chResults)
		found := false
		for res := range chResults {
			if res.Error == nil {
				found = true
			}
		}
		// Only write the records which are not found in other files to the new file
		if !found {
			count++
			w.WriteString(line + "\n")
		}
	}
	// log.Printf("File: %s, count: %d\n", path, count)
	if count > 0 {
		w.WriteString("\n")
		w.WriteString("There are " + strconv.Itoa(count) + " SMS CDRs in all.\n")
	}
	w.Flush()

	return path, nil
}

func grep(file string, reg *regexp.Regexp, wait *sync.WaitGroup, ch chan<- *result) {
	fd, err := os.Open(file)
	if err != nil {
		ch <- &result{
			Filename: file,
			Error:    err,
		}
	}
	bf := bufio.NewScanner(fd)
	var lineno = 1
	for bf.Scan() {
		// There is no XOR in Golang, so you ahve to do this :
		if line := bf.Text(); reg.Match([]byte(line)) {
			ch <- &result{
				Filename:   file,
				Line:       line,
				LineNumber: lineno,
				Error:      nil,
			}
		}
		lineno++
	}
	wait.Done()
}

func renameFile(path string) {
	// Rename the files after update
	os.Rename(path, "."+path+".old")
	os.Rename(path+".new", path)
	log.Println("Updated " + path)
}

func main() {
	var (
		scanFilesPattern   string
		updateFilesPattern string
		workPath           string
	)

	flag.StringVar(&scanFilesPattern, "scan", "", "Files to scan")
	flag.StringVar(&updateFilesPattern, "update", "", "Files to update")
	flag.StringVar(&workPath, "work-path", ".", "Files to update")

	flag.Parse()

	if len(scanFilesPattern) <= 0 {
		log.Panic("Please specify the files to scan.")
	}
	if len(updateFilesPattern) <= 0 {
		log.Panic("Please specify the files to update.")
	}

	log.Printf("Work path: %s\n", workPath)
	log.Printf("Files to scan: %s\n", scanFilesPattern)
	log.Printf("Files to update: %s\n", updateFilesPattern)

	err := os.Chdir(workPath)
	check(err)
	matches, err := filepath.Glob(updateFilesPattern)
	check(err)

	// for _, f := range matches {
	// 	processFile(f, scanFilesPattern)
	// 	renameFile(f)
	// } // 14m15s with sequential loop
	sem := make(chan empty, len(matches)) // semaphore pattern
	for _, f := range matches {
		go func(f string) {
			processFile(f, scanFilesPattern)
			renameFile(f)
			sem <- empty{}
		}(f)
	}
	// wait for goroutines to finish
	for range matches {
		<-sem
	}
	// 3min28s with parallel loop
}
