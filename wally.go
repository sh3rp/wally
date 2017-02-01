package wally

import (
	"errors"
	"os"
	"strconv"
)

const DEFAULT_MAX_DATA_SIZE = 65535
const DEFAULT_MAX_INDEX_ENTRIES = 5000

type Wally struct {
	Name            string
	BaseDir         string
	Index           *MasterIndex
	CurrentIndex    int64
	MaxDataSize     int
	MaxIndexEntries int
}

type WallyConfig struct {
	BaseDir           string
	BaseName          string
	MaxDataSize       int
	MaxIndexEntries   int
	MaxEntryRetention int
}

func DefaultConfig() WallyConfig {
	return WallyConfig{
		BaseDir:           ".",
		BaseName:          "wally",
		MaxDataSize:       DEFAULT_MAX_DATA_SIZE,
		MaxIndexEntries:   DEFAULT_MAX_INDEX_ENTRIES,
		MaxEntryRetention: -1,
	}
}

func NewWally(config WallyConfig) *Wally {
	if _, err := os.Stat(config.BaseDir); err != nil {
		os.MkdirAll(config.BaseDir, 0600)
	}
	path := config.BaseDir + "/" + config.BaseName + ".mdx"

	var masterIndex *MasterIndex

	if _, err := os.Stat(path); err != nil {
		masterIndex = &MasterIndex{Filename: path}
	} else {
		masterIndex, err = ReadMasterIndex(path)
	}

	return &Wally{
		Index:           masterIndex,
		Name:            config.BaseName,
		BaseDir:         config.BaseDir,
		CurrentIndex:    masterIndex.LastIndex().StartOffset,
		MaxDataSize:     config.MaxDataSize,
		MaxIndexEntries: config.MaxIndexEntries,
	}
}

func (w *Wally) Write(data []byte) (int, error) {
	if len(data) > w.MaxDataSize {
		return 0, errors.New("Max data size (" + strconv.Itoa(w.MaxDataSize) + ") exceeded (" + strconv.Itoa(len(data)) + ")")
	}

	var index Index
	index = w.Index.LastIndex()

	if index.Filename == "" {
		index = Index{Filename: w.BaseDir + "/" + w.Name + "-1.idx", BlobFilename: w.BaseDir + "/" + w.Name + "-1.dat"}
	} else if len(index.Records) >= w.MaxIndexEntries {
		index = Index{Filename: w.BaseDir + "/" + w.Name + "-" + strconv.Itoa(len(w.Index.Indices)+1) + ".dat"}
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
