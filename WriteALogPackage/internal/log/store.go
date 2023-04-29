package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mut    sync.Mutex
	buffer *bufio.Writer
	size   uint64
}

func storeCtor(f *os.File) (*store, error) {
	fd, err := os.Stat(f.Name()) // file descriptor for a file
	if err != nil {
		return nil, err
	}

	size := uint64(fd.Size())

	return &store{
		File:   f,
		size:   size,
		buffer: bufio.NewWriter(f),
	}, nil
}

func (st *store) Append(p []byte) (uint64, uint64, error) {
	st.mut.Lock()
	defer st.mut.Unlock()

	pos := st.size // current position in store file

	if err := binary.Write(st.buffer, enc, uint64(len(p))); err != nil { // store the size of record
		return 0, 0, err
	}
	w, err := st.buffer.Write(p) // store the content of the record
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	st.size += uint64(w)

	return uint64(w), pos, nil
}

func (st *store) Read(position uint64) ([]byte, error) {
	st.mut.Lock()
	defer st.mut.Unlock()

	if err := st.buffer.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth) // read in the size of record
	if _, err := st.File.ReadAt(size, int64(position)); err != nil {
		return nil, err
	}

	buffer := make([]byte, enc.Uint64(size))                                    // create a buffer of size SIZE
	if _, err := st.File.ReadAt(buffer, int64(position+lenWidth)); err != nil { // read in the content at position+lenWidth into buffer, size(buffer)
		return nil, err
	}
	return buffer, nil
}

func (st *store) ReadAt(p []byte, off int64) (int, error) {
	st.mut.Lock()
	defer st.mut.Unlock()
	if err := st.buffer.Flush(); err != nil {
		return 0, err
	}
	return st.File.ReadAt(p, off)
}

func (st *store) Close() error {
	st.mut.Lock()
	defer st.mut.Unlock()
	err := st.buffer.Flush()
	if err != nil {
		return err
	}
	return st.File.Close()
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
