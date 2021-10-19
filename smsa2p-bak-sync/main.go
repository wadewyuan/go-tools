package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/sftp"
	SSHTunnel "github.com/wadewyuan/go-tools/ssh"
	"golang.org/x/crypto/ssh"
)

type Endpoint struct {
	Host     string
	Port     int
	Username string
	Password string
}

type Config struct {
	LocalPaths []string

	RemotePaths []string

	Remote Endpoint

	Tunnel Endpoint
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

func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}

func syncFile(path string, tunnel SSHTunnel.SSHTunnel, conf Config) error {

	// Get the index of the file directory in conf.LocalPaths, then get the corresponding remote path to upload file to
	dir, fname := filepath.Split(path)
	log.Print(dir)
	log.Print(fname)
	remoteDir := conf.RemotePaths[indexOf(dir, conf.LocalPaths)]

	addr := fmt.Sprintf("127.0.0.1:%d", tunnel.Local.Port)
	sshConfig := &ssh.ClientConfig{
		User: conf.Remote.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(conf.Remote.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		log.Print(err)
		return err
	}
	defer conn.Close()

	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Print(err)
		return err
	}
	// Close connection
	defer client.Close()

	// Open local file for reading
	srcFile, err := os.Open(path)
	if err != nil {
		log.Print(err)
		return err
	}
	defer srcFile.Close()

	// Add a ".writing" prefix during the uploading process
	dstFile, err := client.Create(remoteDir + "/.writing" + fname)
	if err != nil {
		log.Print(err)
		return err
	}
	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		log.Print(err)
	}
	dstFile.Close()

	// Remove ".writing" prefix when upload complete
	err = client.Rename(remoteDir+"/.writing"+fname, remoteDir+"/"+fname)
	log.Println("Uploaded " + fname)
	if err != nil {
		log.Print(err)
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

	// Create SSH Tunnel
	tunnel := SSHTunnel.NewSSHTunnel(
		// User and host of tunnel server, it will default to port 22
		// if not specified.
		fmt.Sprintf("%s@%s", conf.Tunnel.Username, conf.Tunnel.Host),
		// Password authentication methods
		ssh.Password(conf.Tunnel.Password),
		// The destination host and port of the actual server.
		fmt.Sprintf("%s:%d", conf.Remote.Host, conf.Remote.Port),
	)
	// You can provide a logger for debugging, or remove this line to
	// make it silent.
	tunnel.Log = log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)
	// Start the server in the background. You will need to wait a
	// small amount of time for it to bind to the localhost port
	// before you can start sending connections.
	go tunnel.Start()
	time.Sleep(100 * time.Millisecond)
	// NewSSHTunnel will bind to a random port so that you can have
	// multiple SSH tunnels available. The port is available through:
	//   tunnel.Local.Port

	// You can use any normal Go code to connect to the destination
	// server through localhost. You may need to use 127.0.0.1 for
	// some libraries.

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
				log.Printf("EVENT: %s, OP: %s\n", event.Name, event.Op.String())

				if event.Op == fsnotify.CloseWrite || event.Op == fsnotify.Create {
					err := syncFile(event.Name, *tunnel, conf)
					if err != nil {
						log.Printf("Error sync file: %s\n ", event.Name)
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
