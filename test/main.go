package main

import (
	"os"
	"path/filepath"

	"github.com/jparr721/simpleMPI/mpi"
)

func main() {
	wd, err := os.Getwd()

	if err != nil {
		panic(err)
	}

	ipJson := filepath.Join(wd, "ip.json")
	configJson := filepath.Join(wd, "config.json")

	mpi.WorldInit(ipJson, configJson)
	mpi.Close()
}
