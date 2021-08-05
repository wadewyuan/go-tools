package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/iafan/cwalk"
	_ "github.com/sijms/go-ora"
)

func main() {
	workPath := "/data/media/files/sms_rt_day_bill"
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

			f, _ := os.Open(path)

			scanner := bufio.NewScanner(f)
			scanner.Split(bufio.ScanLines)

			var realExecutions string
			var lines int

			var tMax = time.Time{}
			for scanner.Scan() {
				if strings.Contains(scanner.Text(), "Start SMS daily billing") {
					lines = 1
					realExecutions = scanner.Text()
				} else {
					lines += 1
					realExecutions += "\n"
					realExecutions += scanner.Text()
					if strings.Contains(scanner.Text(), "daily blling end") && lines > 2 {
						realExecutions += "\n"

						t, err := time.Parse("2006-01-02 15:04:05", realExecutions[:19])
						if err != nil {
							log.Printf("Failed to parse time from string: %s\n", realExecutions[:19])
						}

						if tMax.IsZero() || t.After(tMax) {
							tMax = t
						}
					}
				}
			}
			fmt.Printf("Date: %s, Billing Start Time: %s\n", info.Name()[16:24], tMax.Format(time.RFC3339))
			conn, err := sql.Open("oracle", "oracle://wadeyuan:fkeuya@172.18.100.231:1530/devbase")
			if err != nil {
				log.Panic(err)
			}
			defer conn.Close()

			stmt, err := conn.Prepare("INSERT INTO temp_table VALUES(:1, :2)")
			if err != nil {
				log.Panic(err)
			}
			defer stmt.Close()
			d, err := time.Parse("20060102", info.Name()[16:24])
			if err != nil {
				log.Printf("Failed to parse time from string: %s\n", info.Name()[16:24])
			}
			_, err = stmt.Exec(d, tMax)
			if err != nil {
				log.Panic(err)
			}

			f.Close()

			return nil
		})
	if err != nil {
		log.Panic(err)
	}
}
