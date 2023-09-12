package main

import (
	"fmt"
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

	world := mpi.WorldInit(ipJson, configJson)
	fmt.Println("WORLD", world)
	fmt.Println("WORLD", world.IPPool)
	fmt.Println("WORLD", world.Port)
	mpi.Close()
}
