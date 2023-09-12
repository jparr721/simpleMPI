package mpi

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"go.uber.org/zap"
)

type config struct {
	User    string `json:"user"`
	KeyFile string `json:"keyfile"`
	Verbose bool   `json:"verbose"`
}

type host struct {
	// The host address
	Address string `json:"address"`

	// The host role ["master" | "slave"]
	Role *string `json:"role,omitempty"`

	// The directory is where the executable will be as well as
	// where the directory will be changed to. Supports environment
	// variable expansion.
	Directory string `json:"directory"`

	// The name of the executable being run.
	ExeName string `json:"exe_name"`
}

// PathToExecutable returns the path to the executable file based on
// the provided host configuration.
func (h *host) PathToExecutable() string {
	return filepath.Join(h.Directory, h.ExeName)
}

type hostGroup struct {
	Hosts []host `json:"hosts"`
}

type MPIWorld struct {
	size   uint64
	rank   []uint64
	IPPool []string
	Port   []uint64
}

func NewHostGroup(filePath string) (*hostGroup, error) {
	// reading IP from file, the first IP is the master node
	// the rest are the slave nodes
	ipFile, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var group hostGroup
	err = json.Unmarshal(ipFile, &group)
	if err != nil {
		return nil, err
	}

	return &group, nil
}

func (hg *hostGroup) ArrangeHosts(world *MPIWorld) error {
	hosts := hg.Hosts

	masterFound := false
	for _, host := range hosts {
		// The role is optional, as long as we have a master node.
		var role string
		if host.Role == nil {
			role = "node"
		} else {
			role = *host.Role
		}

		if role != "node" && role != "master" {
			zap.L().Error("Invalid Role Found", zap.String("Role", role))
			continue
		}

		if role == "master" {
			masterFound = true
		}

		address := host.Address
		world.IPPool = append(world.IPPool, address)

		// get a random port number betwee 10000 and 20000
		world.rank = append(world.rank, world.size)
		world.size++
	}

	// Make sure that we found a master node
	if !masterFound {
		return errors.New("No master node found")
	}

	return nil
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}

func SerializeWorld(world *MPIWorld) []byte {
	// serialize the MPIWorld struct
	// format: size, rank, IPPool, Port
	// size: uint64
	buf := make([]byte, 0)
	sizebuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(sizebuf, world.size)
	buf = append(buf, sizebuf...)

	// rank: []uint64
	for _, rank := range world.rank {
		rankBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(rankBuf, rank)
		buf = append(buf, rankBuf...)
	}

	// IPPool: []string
	for _, ip := range world.IPPool {
		IPBuf := make([]byte, 0)
		IPBuf = append(IPBuf, []byte(ip)...)
		IPBuf = append(IPBuf, 0)
		buf = append(buf, IPBuf...)
	}

	// Port: []uint64
	for _, port := range world.Port {
		portBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(portBuf, port)
		buf = append(buf, portBuf...)
	}
	return buf
}

func DeserializeWorld(buf []byte) *MPIWorld {
	// deserialize the MPIWorld struct
	// format: size, rank, IPPool, Port
	// size: uint64
	world := new(MPIWorld)
	world.size = binary.LittleEndian.Uint64(buf[:8])
	buf = buf[8:]

	// rank: []uint64
	world.rank = make([]uint64, world.size)
	for i := uint64(0); i < world.size; i++ {
		world.rank[i] = binary.LittleEndian.Uint64(buf[:8])
		buf = buf[8:]
	}

	// IPPool: []string
	world.IPPool = make([]string, world.size)
	for i := uint64(0); i < world.size; i++ {
		end := 0
		for end < len(buf) && buf[end] != 0 {
			end++
		}
		world.IPPool[i] = string(buf[:end])
		buf = buf[end+1:]
	}

	// Port: []uint64
	world.Port = make([]uint64, world.size)
	for i := uint64(0); i < world.size; i++ {
		world.Port[i] = binary.LittleEndian.Uint64(buf[:8])
		buf = buf[8:]
	}
	return world
}

var (
	SelfRank              uint64
	MasterToSlaveTCPConn  []*net.Conn
	SlaveToMasterTCPConn  *net.Conn
	MasterToSlaveListener []*net.Listener
	SlaveOutputs          []bytes.Buffer
	SlaveOutputsErr       []bytes.Buffer
	BytesSent             uint64
	BytesReceived         uint64
	WorldSize             uint64
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

func checkSlave() bool {
	LastCommand := os.Args[len(os.Args)-1]
	return strings.ToLower(LastCommand) == "slave"
}

// ParseConfig parses the config JSON file
//
//	 {
//	 user: string
//	 keyfile: string
//	 verbose: bool
//	}
func ParseConfig(ConfigFilePath string) (config, error) {
	var cfg config
	configFile, err := os.ReadFile(ConfigFilePath)
	if err != nil {
		return cfg, err
	}

	json.Unmarshal(configFile, &cfg)

	return cfg, nil
}

// WorldInit initializes the TCP connections between the main node and the slave nodes.
// It takes as input the HostFilePath which is a newline delimited sequence of IP addresses
// with the host IP first. The format for this file is as follows:
//
//	localhost:9998
//	localhost:9999
//
// The second paramter allows for the specification of the config file which allows for
// the configuration of the slave nodes. An example of this configuration can be seen as
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

	isSlave := checkSlave()
	zap.L().Info("Assigning node position",
		zap.Bool("isSlave", isSlave),
		zap.String("My IPs", strings.Join(selfIP, ",")),
	)

	if !isSlave {
		// Setup TCP connections master <--> slaves
		ConfigureMaster(hostFilePath, configFilePath, world)
	} else {
		ConfigureSlave(world)
	}

	WorldSize = world.size
	return world
}

func ConfigureMaster(hostFilePath, configFilePath string, world *MPIWorld) {
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
		panic("World has no slaves")
	}

	MasterToSlaveTCPConn = make([]*net.Conn, world.size)
	SlaveOutputs = make([]bytes.Buffer, world.size)
	SlaveOutputsErr = make([]bytes.Buffer, world.size)
	MasterToSlaveListener = make([]*net.Listener, world.size)
	MasterToSlaveTCPConn[0] = nil

	// The path of the executable always is assumed to be in the
	// home directory
	// selfFileLocation := "$HOME/simpleMPI/main"

	SelfRank = 0
	for i := 1; i < int(world.size); i++ {
		hostFileLocation := hg.Hosts[i].PathToExecutable()
		slaveIP := world.IPPool[i]
		slavePort := world.Port[i]
		slaveRank := uint64(i)

		// Start slave process via ssh
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
		zap.L().Info("Connecting to slave", zap.String("SlaveIP", slaveIP))
		conn, err := ssh.Dial("tcp", slaveIP+":"+strconv.Itoa(int(22)), &ssh.ClientConfig{
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

		// Listen to slave
		listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(slavePort)))
		if err != nil {
			fmt.Println(err)
			panic("Failed to listen: " + err.Error())
		}
		world.Port[i] = uint64(listener.Addr().(*net.TCPAddr).Port)
		fmt.Println("Slave " + strconv.Itoa(i) + " Listening on port: " + strconv.Itoa(int(world.Port[i])))
		if err != nil {
			fmt.Println(err)
			panic("Failed to listen: " + err.Error())
		}

		session, err := conn.NewSession()
		if err != nil {
			fmt.Println(err)
			panic("Failed to create session: " + err.Error())
		}
		Command := hostFileLocation
		for j := 1; j < len(os.Args); j++ {
			Command += " " + os.Args[j]
		}
		Command += " " + world.IPPool[0] + " " + strconv.Itoa(int(world.Port[i]))
		Command += " Slave"

		zap.L().Info("Preparing command", zap.String("Command", Command))

		stdOutRedirected := make(chan struct{}, 1)
		//run the command async and panic when command return error
		go func() {
			defer session.Close()
			session.Stdout = &SlaveOutputs[i]
			session.Stderr = &SlaveOutputsErr[i]
			close(stdOutRedirected)
			err := session.Run(Command)

			if err != nil {
				fmt.Println("COMMAND ERROR", err)
			} else {
				fmt.Println("STDOUT", session.Stdout)
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
					data, _ := SlaveOutputs[rank].ReadString('\n')
					if data != "" && configuration.Verbose {
						fmt.Println("rank " + strconv.Itoa(int(rank)) + " " + data)
					}
					data, _ = SlaveOutputsErr[rank].ReadString('\n')
					if data != "" {
						ErrorColor := "\033[1;31m%s\033[0m"
						fmt.Printf(ErrorColor, "rank "+strconv.Itoa(int(rank))+" ERR "+data)
					}
					time.Sleep(1 * time.Microsecond)
				}()
			}
		}(uint64(i))

		// Accept a connection
		TCPConn, err := listener.Accept()

		MasterToSlaveTCPConn[i] = &TCPConn
		MasterToSlaveListener[i] = &listener
		if err != nil {
			fmt.Println(err)
			panic("Failed to connect via TCP: " + err.Error())
		}
		fmt.Println("Connected to slave " + strconv.Itoa(i))

		// Send slave rank
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(slaveRank))
		_, err = TCPConn.Write(buf)
		if err != nil {
			fmt.Println(err)
			panic("Failed to send rank: " + err.Error())
		}

		// Send the working directory
		{
			workingDir, err := os.Getwd()
			if err != nil {
				fmt.Println(err)
				panic("Failed to get working directory: " + err.Error())
			}
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
			fmt.Println("Sent working directory to slave " + strconv.Itoa(i))
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

func ConfigureSlave(world *MPIWorld) {
	// Connect to master node
	masterIP := os.Args[len(os.Args)-3]
	slavePort := os.Args[len(os.Args)-2]

	zap.L().Info("Connecting to master node", zap.String("Master IP", masterIP), zap.String("My Port", slavePort))

	TCPConn, err := net.Dial("tcp", masterIP+":"+slavePort)
	SlaveToMasterTCPConn = &TCPConn
	if err != nil {
		fmt.Println(err)
		panic("Failed to accept: " + err.Error())
	}
	// Receive master rank
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
			fmt.Println(err)
			panic("Failed to receive working directory length: " + err.Error())
		}
		workingDirLength := binary.LittleEndian.Uint64(buf)
		//Receive string
		buf = make([]byte, workingDirLength)
		_, err = TCPConn.Read(buf)
		if err != nil {
			fmt.Println(err)
			panic("Failed to receive working directory: " + err.Error())
		}
		workingDir := string(buf)
		err = os.Chdir(workingDir)
		if err != nil {
			fmt.Println(err)
			panic("Failed to change working directory: " + err.Error())
		}
		workingDir, _ = os.Getwd()
		fmt.Println("Changed working directory to " + workingDir)
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

// If Master calls this function, rank is required
// If Slave calls this function, rank is not required, it will send to Master
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
			n, errorMsg = (*MasterToSlaveTCPConn[rank]).Write(buf)
		} else {
			n, errorMsg = (*SlaveToMasterTCPConn).Write(buf)
		}
		if errorMsg != nil {
			fmt.Println(string(debug.Stack()))
			return errorMsg
		}
		BytesSentInThisSession += n
		BytesSent += uint64(n)
		buf = buf[n:]
	}
	return errorMsg
}

// If Master calls this function, rank is required, it will receive from rank-th slave
// If Slave calls this function, rank is not required, it will receive from Master
func ReceiveBytes(size uint64, rank uint64) ([]byte, error) {
	buf := make([]byte, size)
	var errorMsg error
	errorMsg = nil
	BytesRead := uint64(0)
	for BytesRead < size {
		n := 0
		tmpBuf := make([]byte, size-BytesRead)
		if SelfRank == 0 {
			(*MasterToSlaveTCPConn[rank]).SetReadDeadline(time.Now().Add(1000 * time.Second))
			n, errorMsg = (*MasterToSlaveTCPConn[rank]).Read(tmpBuf)
		} else {
			(*SlaveToMasterTCPConn).SetReadDeadline(time.Now().Add(1000 * time.Second))
			n, errorMsg = (*SlaveToMasterTCPConn).Read(tmpBuf)
		}
		for i := BytesRead; i < BytesRead+uint64(n); i++ {
			buf[i] = tmpBuf[i-BytesRead]
		}
		if errorMsg != nil {
			if errorMsg.Error() == "EOF" {
				fmt.Println("EOF")
			}
			fmt.Println(string(debug.Stack()))
			return buf, errorMsg
		}
		BytesReceived += uint64(n)
		BytesRead += uint64(n)
	}
	recvBytes = append(recvBytes, buf...)
	return buf, errorMsg
}

func GetHash(str string) {
	fmt.Println(str + " Bytes sent: " + strconv.Itoa(int(BytesSent)))
	fmt.Println(str + " Bytes received: " + strconv.Itoa(int(BytesReceived)))
	fmt.Println(str + " Sent hash: " + fmt.Sprintf("%x", md5.Sum(sentBytes)))
	fmt.Println(str + " Received hash: " + fmt.Sprintf("%x", md5.Sum(recvBytes)))
}

func Close() {
	fmt.Println("Bytes sent: " + strconv.Itoa(int(BytesSent)))
	fmt.Println("Bytes received: " + strconv.Itoa(int(BytesReceived)))
	fmt.Println("Sent hash: " + fmt.Sprintf("%x", md5.Sum(sentBytes)))
	fmt.Println("Received hash: " + fmt.Sprintf("%x", md5.Sum(recvBytes)))
	if SelfRank == 0 {
		time.Sleep(1 * time.Second)
		for i := 1; i < len(MasterToSlaveTCPConn); i++ {
			(*MasterToSlaveTCPConn[i]).Close()
			(*MasterToSlaveListener[i]).Close()
		}
	} else {
		(*SlaveToMasterTCPConn).Close()
	}
}
