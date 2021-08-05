package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
)

func main() {
	err := os.Chdir("/data/media/files/CTK")
	if err != nil {
		panic(err)
	}

	// grep := exec.Command("/bin/grep", "2021-06-07 16:50:15,2021-06-07 16:50:19,4,0,,852645010008,0,0,22,0,0,0,85262264935019736,85255679727,.*,CTK,99999,0", "*HKG*")
	cmd := exec.Command("bash", "-c", "grep '2021-06-07 16:50:15,2021-06-07 16:50:19,4,0,,852645010008,0,0,22,0,0,0,85262264935019736,85255679727,.*,CTK,99999,0' *HKG*")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}

	if err := cmd.Start(); err != nil {
		panic(err)
	}

	//读取所有输出
	bytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		fmt.Println("ReadAll Stdout:", err.Error())
		return
	}

	if err := cmd.Wait(); err != nil {
		fmt.Println("wait:", err.Error())
		return
	}
	fmt.Printf("stdout:\n\n %s", bytes)
}
