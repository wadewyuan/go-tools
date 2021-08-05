package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/iafan/cwalk"
	gzip "github.com/klauspost/pgzip"
)

func main() {
	start := time.Now()
	argsWithoutProg := os.Args[1:]
	workPath := argsWithoutProg[0]
	err := os.Chdir(workPath)
	if err != nil {
		log.Println(err)
	}
	err = cwalk.Walk(workPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || strings.HasSuffix(info.Name(), ".gz") {
				return nil
			}
			// fmt.Println(path, info.Size())
			fmt.Printf("Gzipping file %s\n", info.Name())
			f, _ := os.Open(path)

			reader := bufio.NewReader(f)
			content, _ := ioutil.ReadAll(reader)
			// Add gz extension.
			zippedFilePath := path + ".gz"
			// Open file for writing.
			f, _ = os.Create(zippedFilePath)
			// Write compressed data.
			w := gzip.NewWriter(f)
			w.Write(content)
			w.Close()
			return nil
		})
	if err != nil {
		log.Println(err)
	}
	duration := time.Since(start)
	fmt.Println(duration.Nanoseconds())
}
