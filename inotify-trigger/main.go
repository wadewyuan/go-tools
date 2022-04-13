package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/fsnotify/fsnotify"
	alarm "github.com/wadewyuan/smartom-utils-go"
)

type Config struct {
	LocalEvent string

	LocalPaths []string

	Pattern string

	PostCommand string

	AlarmCode string
}

var watcher *fsnotify.Watcher

// watchDir gets run as a walk func, searching for directories to add watchers to
func watchDir(path string, fi os.FileInfo, err error) error {

	// since fsnotify can watch all the files in a directory, watchers only need
	// to be added to each nested directory
	if fi.Mode().IsDir() {
		return watcher.Add(path)
	}

	return nil
}

func postProcess(path string, conf Config) error {
	matched, err := regexp.MatchString(conf.Pattern, path)
	if matched {
		if stat, err := os.Stat(path); err == nil {
			log.Printf("Processing file: %s, size: %d", path, stat.Size())
			command := strings.Replace(conf.PostCommand, "$FILE_NAME", path, 1) // Replace the placeholder with file name
			log.Printf("Command: %s", command)
			cmd := exec.Command("/bin/bash", "-c", command)
			if err := cmd.Start(); err != nil {
				log.Println("Execute failed when Start:" + err.Error())
			}
		} else if errors.Is(err, os.ErrNotExist) {
			log.Println(err.Error())
		} else {
			log.Println(err.Error())
		}
	}
	return err
}

func main() {
	var c string
	var conf Config

	// load configuration file
	flag.StringVar(&c, "c", "./config.json", "Specify the configuration file.")
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
	for _, p := range conf.LocalPaths {

		if err := filepath.Walk(p, watchDir); err != nil {
			log.Println("ERROR", err)
		} else {
			log.Printf("Watching %s\n", p)
		}
	}

	//
	done := make(chan bool)

	//
	go func() {
		for {
			select {
			// watch for events
			case event := <-watcher.Events:
				var op = fsnotify.Create
				if len(conf.LocalEvent) > 0 {
					switch conf.LocalEvent {
					case "CLOSEWRITE":
						op = fsnotify.CloseWrite
					case "CREATE":
						op = fsnotify.Create
					case "REMOVE":
						op = fsnotify.Remove
					case "WRITE":
						op = fsnotify.Write
					case "CHMOD":
						op = fsnotify.Chmod
					case "RENAME":
						op = fsnotify.Rename
					default:
						op = fsnotify.Create
					}
				}

				if event.Op == op {
					err := postProcess(event.Name, conf)
					if err != nil {
						var msg = fmt.Sprintf("Error processing file: %s ", event.Name)
						log.Println(msg, err)
						if len(conf.AlarmCode) > 0 {
							alarm.SendAlarm(conf.AlarmCode, msg)
						}
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
