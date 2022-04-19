package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"os"
)

// func lineCounter(r io.Reader) (int, error) {
// 	buf := make([]byte, 32*1024)
// 	count := 0
// 	lineSep := []byte{'\n'}

// 	for {
// 		c, err := r.Read(buf)
// 		count += bytes.Count(buf[:c], lineSep)

// 		switch {
// 		case err == io.EOF:
// 			return count, nil

// 		case err != nil:
// 			return count, err
// 		}
// 	}
// }

func main() {
	var fileName string
	var multiple int

	// Parse command line params
	flag.StringVar(&fileName, "f", "", "Specify the file name to inflate")
	flag.IntVar(&multiple, "m", 1, "The multiple of file to inflate")
	flag.Parse()

	if multiple < 2 {
		log.Panicf("Invalid multiple %d, it must be bigger than 1", multiple)
	}

	if f, err := os.Open(fileName); err == nil {
		defer f.Close()
		b, err := io.ReadAll(f)
		if err != nil {
			log.Println(err.Error())
		}
		// origLineNum := bytes.Count(b, []byte{'\n'})
		// log.Printf("Total bytes: %d", len(b))
		// log.Printf("Total lines: %d", origLineNum)
		if len(b) > 0 {
			newFile, err := os.Create(fileName + ".new")
			if err != nil {
				log.Panic(err.Error())
			}
			defer newFile.Close()
			// Create a writer
			w := bufio.NewWriter(newFile)
			for multiple > 0 {
				multiple -= 1
				w.Write(b)
			}
			w.Flush()
		}

		// scanner := bufio.NewScanner(f)
		// i := 0
		// for scanner.Scan() {
		// 	if i <
		// 	i++
		// }

	} else {
		log.Println(err.Error())
	}
}
