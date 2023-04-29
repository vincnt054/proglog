package log

import (
	"enc"
	"io"
	"os"

	"github.com/tysontate/gommap"
)

var (
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entryWidth           = offsetWidth + positionWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap // memory map
	size uint64
}

func indexCtor(f *os.File, c Config) (*index, error) {
	indexT := &index{
		file: f,
	}

	fd, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	indexT.size = uint64(fd.Size())

	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if indexT.mmap, err = gommap.Map(
		indexT.file.Fd(),
		gommap.PROT_HEAD|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return indexT, nil
}

func (ix *index) Close() error {
	if err := ix.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := ix.file.Sync(); err != nil {
		return err
	}

	if err := ix.file.Truncate(int64(ix.size)); err != nil {
		return err
	}

	return ix.file.Close()
}

func (ix *index) Read(input int64) (output uint32, position uint64, err error) {
	if ix.size == 0 {
		return 0, 0, io.EOF
	}

	if input == -1 {
		output = uint32((ix.size / entryWidth) - 1)
	} else {
		output = uint32(input)
	}

	position = uint64(output) * entryWidth
	if ix.size < position+entryWidth {
		return 0, 0, io.EOF
	}

	output = enc.Uint32(ix.mmap[position : position+offsetWidth])
	position = enc.Uint64(ix.mmap[position+offsetWidth : position+entryWidth])
	return output, position, nil
}

func (ix *index) Write(offset uint32, position uint64) error {
	if uint64(len(ix.mmap)) < ix.size+entryWidth {
		return io.EOF
	}

	enc.PutUint32(ix.mmap[ix.size:ix.size+offsetWidth], offset)
	enc.PutUint64(ix.mmap[ix.size+offsetWidth:ix.size+entryWidth], position)

	ix.size += uint64(entryWidth)
	return nil
}

func (ix *index) Name() string {
	return ix.file.Name()
}
