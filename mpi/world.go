package mpi

import "encoding/binary"

type MPIWorld struct {
	size   uint64
	rank   []uint64
	IPPool []string
	Port   []uint64
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
