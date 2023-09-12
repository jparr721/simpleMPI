package main

import (
	"fmt"

	"github.com/jparr721/simpleMPI/mpi"
)

func main() {
	world := mpi.WorldInit("/Users/jarredparr/Documents/Projects/zkp.nosync/simpleMPI/test/ip.json",
		"/Users/jarredparr/Documents/Projects/zkp.nosync/simpleMPI/test/config.json")
	fmt.Println("WORLD", world)
	fmt.Println("WORLD", world.IPPool)
	fmt.Println("WORLD", world.Port)
	mpi.Close()
}
