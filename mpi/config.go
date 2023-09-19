package mpi

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type config struct {
	User    string `json:"user"`
	KeyFile string `json:"keyfile"`
	Verbose bool   `json:"verbose,string"`
}

type host struct {
	// The host address
	Address string `json:"address"`

	// The host role ["dispatcher" | "worker"]
	Role *string `json:"role,omitempty"`

	// The directory is where the executable will be as well as
	// where the directory will be changed to. Supports environment
	// variable expansion.
	Directory string `json:"directory"`

	// The name of the executable being run.
	ExeName string `json:"exe_name"`

	// The optional port of the host (Default 22)
	Port *int `json:"port,omitempty,string"`
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

// ArrangeHosts takes the host group and a world pointer and assigns the roles and
// their respective places in the MPIWorld struct. It also checks to make sure that
// the appropriate nodes have been specified.
func (hg *hostGroup) ArrangeHosts(world *MPIWorld) error {
	hosts := hg.Hosts

	dispatcherFound := false
	for _, host := range hosts {
		// The role is optional, as long as we have a dispatcher node.
		var role string
		if host.Role == nil {
			role = "worker"
		} else {
			role = *host.Role
		}

		if role != "worker" && role != "dispatcher" {
			continue
		}

		if role == "dispatcher" {
			dispatcherFound = true
		}

		address := host.Address
		world.IPPool = append(world.IPPool, address)

		// get a random port number betwee 10000 and 20000
		world.rank = append(world.rank, world.size)
		world.size++
	}

	// Make sure that we found a dispatcher node
	if !dispatcherFound {
		return errors.New("No dispatcher node found")
	}

	return nil
}
