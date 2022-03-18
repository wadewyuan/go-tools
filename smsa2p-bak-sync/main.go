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
	alarm "github.com/wadewyuan/smartom-utils-go"
	"golang.org/x/crypto/ssh"
)

type Endpoint struct {
	Host     string
	Port     int
	Username string
	Password string
}

type Config struct {
	LocalEvent string

	LocalPaths []string

	RemotePaths []string

	Remote Endpoint

	Tunnel Endpoint

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

func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}

func syncFile(path string, tunnel *SSHTunnel.SSHTunnel, conf Config) error {

	// Get the index of the file directory in conf.LocalPaths, then get the corresponding remote path to upload file to
	dir, fname := filepath.Split(path)
	remoteDir := conf.RemotePaths[indexOf(dir, conf.LocalPaths)]

	// Open local file and copy it to /tmp to avoic conflicts
	srcFile, err := os.Open(path)
	if err != nil {
		log.Print(err)
		return err
	}
	var srcBytes int64
	fi, err := srcFile.Stat()
	if err != nil {
		log.Print(err)
		srcBytes = 0
		return err
	}
	srcBytes = fi.Size()
	tmpFile, err := os.Create("/tmp/" + fname)
	if err != nil {
		log.Print(err)
		return err
	}
	_, err = io.Copy(tmpFile, srcFile)
	if err != nil {
		log.Print(err)
		return err
	}
	srcFile.Close()
	tmpFile.Close()

	var addr string
	if tunnel == nil {
		addr = fmt.Sprintf("%s:%d", conf.Remote.Host, conf.Remote.Port)
	} else {
		addr = fmt.Sprintf("127.0.0.1:%d", tunnel.Local.Port)
	}
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

	// Add a ".writing" prefix during the uploading process
	dstFile, err := client.Create(remoteDir + "/.writing" + fname)
	if err != nil {
		log.Print(err)
		return err
	}
	// Copy the tmpFile to remote path
	tmpFile, _ = os.Open("/tmp/" + fname)
	nBytes, err := io.Copy(dstFile, tmpFile)
	if err != nil {
		log.Print(err)
	}
	dstFile.Close()
	tmpFile.Close()

	os.Remove("/tmp/" + fname)

	if nBytes != srcBytes {
		return fmt.Errorf("file not fully synced. total bytes:%d, synced bytes:%d", srcBytes, nBytes)
	}
	// Remove ".writing" prefix when upload complete
	err = client.Rename(remoteDir+"/.writing"+fname, remoteDir+"/"+fname)
	if err != nil {
		return err
	}
	log.Println("Synced " + fname)

	return err
}

func main() {
	var c string
	var conf Config
	var tunnel *SSHTunnel.SSHTunnel

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

	if len(conf.Tunnel.Host) > 0 {
		// Create SSH Tunnel
		tunnel = SSHTunnel.NewSSHTunnel(
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
	}

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

				var op = fsnotify.Create
				if len(conf.LocalEvent) > 0 {
					switch conf.LocalEvent {
					case "CLOSEWRITE":
						op = fsnotify.CloseWrite
					case "CREATE":
						op = fsnotify.Create
					default:
						op = fsnotify.Create
					}
				}

				if event.Op == op {
					err := syncFile(event.Name, tunnel, conf)
					if err != nil {
						var msg = fmt.Sprintf("Error sync file: %s ", event.Name)
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
