package mpi

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"golang.org/x/crypto/ssh"

	"go.uber.org/zap"
)

func ConfigureDispatcher(hostFilePath, configFilePath string, world *MPIWorld) {
	configuration, err := ParseConfig(configFilePath)
	if err != nil {
		panic(err)
	}
	zap.L().Info("Configuration loaded successfully",
		zap.String("KeyFile", configuration.KeyFile),
		zap.String("User", configuration.User))

	hg, err := SetIPPool(hostFilePath, world)
	if err != nil {
		panic(err)
	}

	world.Port = make([]uint64, world.size)
	if world.size == 0 {
		panic("World has no workers")
	}

	DispatcherToWorkerTCPConn = make([]*net.Conn, world.size)
	WorkerOutputs = make([]bytes.Buffer, world.size)
	WorkerOutputsErr = make([]bytes.Buffer, world.size)
	DispatcherToWorkerListener = make([]*net.Listener, world.size)
	DispatcherToWorkerTCPConn[0] = nil

	SelfRank = 0
	for i := 1; i < int(world.size); i++ {
		executableFileLocation := hg.Hosts[i].PathToExecutable()
		workerIP := world.IPPool[i]
		workerSshPort := 22
		if hg.Hosts[i].Port != nil {
			workerSshPort = *hg.Hosts[i].Port
		}
		workerSshAddress := workerIP + ":" + strconv.Itoa(workerSshPort)
		// workerSshPort := hg.Hosts[i].Port
		workerPort := world.Port[i]
		workerRank := uint64(i)

		// Start worker process via ssh
		key, err := os.ReadFile(configuration.KeyFile)
		if err != nil {
			fmt.Printf("unable to read private key: %v\n", err)
			panic("Failed to load key")
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			fmt.Printf("unable to parse private key: %v\n", err)
			panic("Failed to parse key")
		}

		zap.L().Info("Connecting to worker", zap.String("workerSshAddress", workerSshAddress))

		conn, err := ssh.Dial("tcp", workerSshAddress, &ssh.ClientConfig{
			User: configuration.User,
			Auth: []ssh.AuthMethod{
				ssh.PublicKeys(signer),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		})

		if err != nil {
			fmt.Println(err)
			panic("Failed to dial: " + err.Error())
		}

		// Listen to worker
		listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(workerPort)))
		if err != nil {
			fmt.Println(err)
			panic("Failed to listen: " + err.Error())
		}
		world.Port[i] = uint64(listener.Addr().(*net.TCPAddr).Port)
		fmt.Println("Worker " + strconv.Itoa(i) + " Listening on port: " + strconv.Itoa(int(world.Port[i])))
		if err != nil {
			fmt.Println(err)
			panic("Failed to listen: " + err.Error())
		}

		session, err := conn.NewSession()
		if err != nil {
			fmt.Println(err)
			panic("Failed to create session: " + err.Error())
		}
		Command := executableFileLocation
		for j := 1; j < len(os.Args); j++ {
			Command += " " + os.Args[j]
		}
		Command += " " + world.IPPool[0] + " " + strconv.Itoa(int(world.Port[i]))
		Command += " Worker"

		zap.L().Info("Preparing command", zap.String("Command", Command))

		stdOutRedirected := make(chan struct{}, 1)
		//run the command async and panic when command return error
		go func() {
			defer session.Close()
			session.Stdout = &WorkerOutputs[i]
			session.Stderr = &WorkerOutputsErr[i]
			close(stdOutRedirected)
			err := session.Run(Command)

			if err != nil {
				zap.L().Error("COMMAND ERROR", zap.String("Error", err.Error()))
			} else {
				zap.L().Info("", zap.String("stdout", fmt.Sprint(session.Stdout)))
			}
		}()

		go func(rank uint64) {
			// Print the output of the command
			<-stdOutRedirected
			for {
				func() {
					defer func() {
						if r := recover(); r != nil {
							fmt.Println("Output err at rank", rank)
						}
						time.Sleep(1 * time.Second)
					}()
					data, _ := WorkerOutputs[rank].ReadString('\n')
					if data != "" && configuration.Verbose {
						fmt.Println("rank " + strconv.Itoa(int(rank)) + " " + data)
					}
					data, _ = WorkerOutputsErr[rank].ReadString('\n')
					if data != "" {
						zap.L().Info("Command Output",
							zap.String("rank", strconv.Itoa(int(rank))),
							zap.String("message", data),
						)
					}
					time.Sleep(1 * time.Microsecond)
				}()
			}
		}(uint64(i))

		// Accept a connection
		TCPConn, err := listener.Accept()

		DispatcherToWorkerTCPConn[i] = &TCPConn
		DispatcherToWorkerListener[i] = &listener
		if err != nil {
			fmt.Println(err)
			panic("Failed to connect via TCP: " + err.Error())
		}
		fmt.Println("Connected to worker " + strconv.Itoa(i))

		// Send worker rank
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(workerRank))
		_, err = TCPConn.Write(buf)
		if err != nil {
			fmt.Println(err)
			panic("Failed to send rank: " + err.Error())
		}

		// Send the working directory
		{
			workingDir := hg.Hosts[i].Directory

			//Send string length
			buf = make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(len(workingDir)))
			_, err = TCPConn.Write(buf)
			if err != nil {
				fmt.Println(err)
				panic("Failed to send working directory length: " + err.Error())
			}
			//Send string
			_, err = TCPConn.Write([]byte(workingDir))
			if err != nil {
				fmt.Println(err)
				panic("Failed to send working directory: " + err.Error())
			}
			fmt.Println("Sent working directory to worker " + strconv.Itoa(i))
		}

		// Sync the world state
		buf = SerializeWorld(world)

		//Send buf size
		bufSize := make([]byte, 8)
		binary.LittleEndian.PutUint64(bufSize, uint64(len(buf)))
		_, err = TCPConn.Write(bufSize)
		if err != nil {
			fmt.Println(err)
			panic("Failed to send buf size: " + err.Error())
		}

		//Send buf
		_, err = TCPConn.Write(buf)
		if err != nil {
			fmt.Println(err)
			panic("Failed to send world: " + err.Error())
		}

	}
}
