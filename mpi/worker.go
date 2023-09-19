package mpi

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"

	"go.uber.org/zap"
)

func ConfigureWorker(world *MPIWorld) {
	// Connect to dispatcher node
	dispatcherIP := os.Args[len(os.Args)-3]
	workerPort := os.Args[len(os.Args)-2]

	zap.L().Info("Connecting to dispatcher node", zap.String("Dispatcher IP", dispatcherIP), zap.String("My Port", workerPort))

	TCPConn, err := net.Dial("tcp", dispatcherIP+":"+workerPort)
	WorkerToDispatcherTCPConn = &TCPConn
	if err != nil {
		fmt.Println(err)
		panic("Failed to accept: " + err.Error())
	}
	// Receive dispatcher rank
	buf := make([]byte, 8)
	_, err = TCPConn.Read(buf)
	if err != nil {
		fmt.Println(err)
		panic("Failed to receive rank: " + err.Error())
	}
	SelfRank = binary.LittleEndian.Uint64(buf)

	// Receive the working directory
	{
		//Receive string length
		buf = make([]byte, 8)
		_, err = TCPConn.Read(buf)
		if err != nil {
			zap.L().Error(err.Error())
			panic("Failed to receive working directory length: " + err.Error())
		}
		workingDirLength := binary.LittleEndian.Uint64(buf)
		//Receive string
		buf = make([]byte, workingDirLength)
		_, err = TCPConn.Read(buf)
		if err != nil {
			zap.L().Error(err.Error())
			panic("Failed to receive working directory: " + err.Error())
		}
		workingDir := string(buf)
		err = os.Chdir(workingDir)
		if err != nil {
			zap.L().Error(err.Error())
			panic("Failed to change working directory: " + err.Error())
		}
		workingDir, _ = os.Getwd()
		zap.L().Info("Changed working directory to " + workingDir)
	}

	// Sync the world state
	// Receive buf size
	bufSize := make([]byte, 8)
	_, err = TCPConn.Read(bufSize)
	if err != nil {
		fmt.Println(err)
		panic("Failed to receive buf size: " + err.Error())
	}
	buf = make([]byte, binary.LittleEndian.Uint64(bufSize))
	fmt.Println("Received buf size " + strconv.Itoa(int(binary.LittleEndian.Uint64(bufSize))))

	_, err = TCPConn.Read(buf)
	if err != nil {
		fmt.Println(err)
		panic("Failed to receive world: " + err.Error())
	}
	world = DeserializeWorld(buf)
}
