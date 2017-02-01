package wally

import (
	"errors"
	"os"
	"strconv"
)

const DEFAULT_MAX_DATA_SIZE = 65535

type Wally struct {
	Name         string
	BaseDir      string
	Index        *MasterIndex
	CurrentIndex int64
	MaxDataSize  int
}

func NewWally(dir string, name string) *Wally {
	if _, err := os.Stat(dir); err != nil {
		os.MkdirAll(dir, 0600)
	}
	path := dir + "/" + name + ".mdx"

	var masterIndex *MasterIndex

	if _, err := os.Stat(path); err != nil {
		masterIndex = &MasterIndex{Filename: path}
	} else {
		masterIndex, err = ReadMasterIndex(path)
	}

	return &Wally{Index: masterIndex, Name: name, BaseDir: dir, CurrentIndex: 0, MaxDataSize: DEFAULT_MAX_DATA_SIZE}
}

func (w *Wally) Write(data []byte) (int, error) {
	if len(data) > w.MaxDataSize {
		return 0, errors.New("Max data size (" + strconv.Itoa(w.MaxDataSize) + ") exceeded (" + strconv.Itoa(len(data)) + ")")
	}

	var index Index
	index = w.Index.LastIndex()

	if index.Filename == "" {
		index = Index{Filename: w.BaseDir + "/" + w.Name + "-1.idx", BlobFilename: w.BaseDir + "/" + w.Name + "-1.dat"}
	}

	err := w.Index.WriteIndex(index)

	if err != nil {
		return 0, nil
	}

	index, err = w.Index.Write(index, data)

	return len(index.Records), err
}

func (w *Wally) Peek(idx int64) ([]byte, error) {
	var index Index
	curIdx := idx
	for _, i := range w.Index.Indices {
		index = i
		if index.StartOffset >= curIdx {
			break
		} else {
			curIdx = curIdx - index.StartOffset
		}
	}

	return w.Index.Read(index, curIdx)
}

func (w *Wally) Next() ([]byte, error) {
	b, e := w.Peek(w.CurrentIndex)
	w.CurrentIndex++
	return b, e
}
