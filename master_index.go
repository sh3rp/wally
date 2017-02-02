package wally

import (
	"encoding/gob"
	"log"
	"os"
)

type MasterIndex struct {
	Filename      string
	CurrentOffset int32
	Indices       []IndexPtr
	indexCache    []Index
}

type IndexPtr struct {
	Filename    string
	StartOffset int32
}

// LastIndex returns the last index in the cache
func (master *MasterIndex) LastIndex() Index {
	return master.indexCache[len(master.indexCache)-1]
}

// AddIndex adds a new index to the slice of indices for this master
func (master *MasterIndex) AddIndex(filename string, index Index) {
	idx := IndexPtr{
		Filename:    filename,
		StartOffset: index.StartOffset,
	}
	master.Indices = append(master.Indices, idx)
	master.refreshIndexCache(len(master.Indices) - 1)
}

// Write persists the master index to disk
func (master *MasterIndex) Write() error {
	f, err := os.OpenFile(master.Filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	err = enc.Encode(master)
	f.Close()
	return err
}

// Read reads the master index from disk
func (master *MasterIndex) Read() error {
	f, err := os.OpenFile(master.Filename, os.O_RDWR, 0600)

	if err != nil {
		return err
	}

	dec := gob.NewDecoder(f)
	err = dec.Decode(master)
	f.Close()
	return err
}

func (master *MasterIndex) refreshAllIndexCache() {
	master.indexCache = make([]Index, len(master.indexCache))
	for i := range master.Indices {
		master.refreshIndexCache(i)
	}
}

func (master *MasterIndex) refreshIndexCache(indexNum int) {
	index := Index{}
	idx := master.Indices[indexNum]
	err := index.Read(idx.Filename)
	if err != nil {
		log.Printf("Error reading index: %v", err)
	}
	master.indexCache[indexNum] = index
}
