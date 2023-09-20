package mpi

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewDevelopment()))
}

var (
	SelfRank                   uint64
	DispatcherToWorkerTCPConn  []*net.Conn
	WorkerToDispatcherTCPConn  *net.Conn
	DispatcherToWorkerListener []*net.Listener
	WorkerOutputs              []bytes.Buffer
	WorkerOutputsErr           []bytes.Buffer
	BytesSent                  uint64
	BytesReceived              uint64
	WorldSize                  uint64
)

// SetIPPool unpacks the json file `filePath` and unrolls the ip groups
// into the world and sets the rank and size as appropriate.
func SetIPPool(filePath string, world *MPIWorld) (*hostGroup, error) {
	hg, err := NewHostGroup(filePath)
	if err != nil {
		return nil, err
	}
	return hg, hg.ArrangeHosts(world)
}

func GetLocalIP() ([]string, error) {
	// get local IP address
	addrs, err := net.InterfaceAddrs()
	result := make([]string, 0)
	if err != nil {
		return result, err
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok {
			if ipnet.IP.To4() != nil {
				result = append(result, ipnet.IP.String())
			}
		}
	}
	return result, nil
}

func checkSelfIsWorker() bool {
	LastCommand := os.Args[len(os.Args)-1]
	return strings.ToLower(LastCommand) == "worker"
}

// ParseConfig parses the config JSON file
//
//	 {
//	 user: string
//	 keyfile: string
//	 verbose: bool
//	}
func ParseConfig(ConfigFilePath string) (*config, error) {
	cfg := &config{}
	configFile, err := os.ReadFile(ConfigFilePath)
	if err != nil {
		return cfg, err
	}

	err = json.Unmarshal(configFile, &cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// WorldInit initializes the TCP connections between the main node and the worker nodes.
// It takes as input the HostFilePath which is a newline delimited sequence of IP addresses
// with the host IP first. The format for this file is as follows:
//
//	localhost:9998
//	localhost:9999
//
// The second paramter allows for the specification of the config file which allows for
// the configuration of the worker nodes. An example of this configuration can be seen as
// follows:
//
//	{
//	user "my_username"
//	keyfile "$HOME/.ssh/private_key_file
//	verbose true
//	}
func WorldInit(hostFilePath, configFilePath string) *MPIWorld {
	zap.L().Info("Initializing MPI World",
		zap.String("HostFilePath", hostFilePath),
		zap.String("ConfigFilePath", configFilePath),
	)
	world := new(MPIWorld)
	world.size = 0
	world.rank = make([]uint64, 0)
	world.IPPool = make([]string, 0)
	world.Port = make([]uint64, 0)

	selfIP, _ := GetLocalIP()

	isWorker := checkSelfIsWorker()
	zap.L().Info("Assigning node position",
		zap.Bool("isWorker", isWorker),
		zap.String("My IPs", strings.Join(selfIP, ",")),
	)

	if !isWorker {
		// Setup TCP connections dispatcher <--> workers
		ConfigureDispatcher(hostFilePath, configFilePath, world)
	} else {
		ConfigureWorker(world)
	}

	WorldSize = world.size
	return world
}

// If Dispatcher calls this function, rank is required
// If Worker calls this function, rank is not required, it will send to Dispatcher
var sentBytes []byte
var recvBytes []byte

func SendBytes(buf []byte, rank uint64) error {
	var errorMsg error
	errorMsg = nil
	BytesSentInThisSession := 0
	sentBytes = append(sentBytes, buf...)
	for len(buf) > 0 {
		n := 0
		if SelfRank == 0 {
			n, errorMsg = (*DispatcherToWorkerTCPConn[rank]).Write(buf)
		} else {
			n, errorMsg = (*WorkerToDispatcherTCPConn).Write(buf)
		}
		if errorMsg != nil {
			zap.L().Info(string(debug.Stack()))
			return errorMsg
		}
		BytesSentInThisSession += n
		BytesSent += uint64(n)
		buf = buf[n:]
	}
	return errorMsg
}

// If Dispatcher calls this function, rank is required, it will receive from rank-th worker
// If Worker calls this function, rank is not required, it will receive from Dispatcher
func ReceiveBytes(size uint64, rank uint64) ([]byte, error) {
	buf := make([]byte, size)
	var errorMsg error
	errorMsg = nil
	BytesRead := uint64(0)
	for BytesRead < size {
		n := 0
		tmpBuf := make([]byte, size-BytesRead)
		if SelfRank == 0 {
			(*DispatcherToWorkerTCPConn[rank]).SetReadDeadline(time.Now().Add(1000 * time.Second))
			n, errorMsg = (*DispatcherToWorkerTCPConn[rank]).Read(tmpBuf)
		} else {
			(*WorkerToDispatcherTCPConn).SetReadDeadline(time.Now().Add(1000 * time.Second))
			n, errorMsg = (*WorkerToDispatcherTCPConn).Read(tmpBuf)
		}
		for i := BytesRead; i < BytesRead+uint64(n); i++ {
			buf[i] = tmpBuf[i-BytesRead]
		}
		if errorMsg != nil {
			if errorMsg.Error() == "EOF" {
				zap.L().Info("EOF")
			}
			zap.L().Info(string(debug.Stack()))
			return buf, errorMsg
		}
		BytesReceived += uint64(n)
		BytesRead += uint64(n)
	}
	recvBytes = append(recvBytes, buf...)
	return buf, errorMsg
}

func GetHash(str string) {
	zap.L().Info(str + " Bytes sent: " + strconv.Itoa(int(BytesSent)))
	zap.L().Info(str + " Bytes received: " + strconv.Itoa(int(BytesReceived)))
	zap.L().Info(str + " Sent hash: " + fmt.Sprintf("%x", md5.Sum(sentBytes)))
	zap.L().Info(str + " Received hash: " + fmt.Sprintf("%x", md5.Sum(recvBytes)))
}

func Close() {
	zap.L().Info("Bytes sent: " + strconv.Itoa(int(BytesSent)))
	zap.L().Info("Bytes received: " + strconv.Itoa(int(BytesReceived)))
	zap.L().Info("Sent hash: " + fmt.Sprintf("%x", md5.Sum(sentBytes)))
	zap.L().Info("Received hash: " + fmt.Sprintf("%x", md5.Sum(recvBytes)))
	if SelfRank == 0 {
		time.Sleep(1 * time.Second)
		for i := 1; i < len(DispatcherToWorkerTCPConn); i++ {
			(*DispatcherToWorkerTCPConn[i]).Close()
			(*DispatcherToWorkerListener[i]).Close()
		}
	} else {
		(*WorkerToDispatcherTCPConn).Close()
	}
}
