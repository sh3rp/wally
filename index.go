package wally

import (
	"encoding/gob"
	"os"
)

type MasterIndex struct {
	Filename string
	Indices  []Index
}

type Index struct {
	Filename     string
	BlobFilename string
	StartOffset  int64
	Records      []int64
}

func WriteMasterIndex(index *MasterIndex) error {
	f, err := os.OpenFile(index.Filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	err = enc.Encode(index)
	f.Close()
	return err
}

func ReadMasterIndex(filename string) (*MasterIndex, error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0600)

	if err != nil {
		return nil, err
	}

	dec := gob.NewDecoder(f)
	var mi MasterIndex
	err = dec.Decode(&mi)
	return &mi, err
}

func (mi *MasterIndex) LastIndex() Index {
	var index Index
	if len(mi.Indices) == 0 {
		return index
	}
	return mi.Indices[len(mi.Indices)-1]
}

func (mi *MasterIndex) WriteIndex(index Index) error {
	var idx *Index
	for a, i := range mi.Indices {
		if i.Filename == index.Filename {
			mi.Indices[a] = index
			idx = &i
		}
	}
	if idx == nil {
		mi.Indices = append(mi.Indices, index)
		idx = &index
	}
	err := WriteMasterIndex(mi)

	if err != nil {
		return err
	}

	f, err := os.OpenFile(index.Filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)

	if err != nil {
		return err
	}

	enc := gob.NewEncoder(f)
	err = enc.Encode(index)
	f.Close()
	return err
}

func (mi *MasterIndex) Write(index Index, data []byte) (Index, error) {
	var idx Index
	file, err := os.OpenFile(index.BlobFilename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return idx, err
	}
	cursor, err := file.Seek(0, os.SEEK_END)
	if cursor > 0 {
		cursor++
	}
	if err != nil {
		return idx, err
	}
	_, err = file.Write(data)
	if err != nil {
		return idx, err
	}
	file.Close()
	index.Records = append(index.Records, cursor)

	mi.WriteIndex(index)
	idx = index
	return idx, nil
}

func (mi *MasterIndex) Read(index Index, pos int64) ([]byte, error) {
	file, err := os.OpenFile(index.BlobFilename, os.O_RDONLY|os.O_CREATE, 0600)

	if err != nil {
		return nil, err
	}

	var start int64
	if pos > 0 {
		start, err = file.Seek(index.Records[pos]-1, 0)
		if err != nil {
			return nil, err
		}
	} else {
		start = 0
	}

	var end int64
	if len(index.Records) > int(pos+1) {
		end, err = file.Seek(index.Records[pos+1], 0)
		if err != nil {
			return nil, err
		}
		end--
	} else {
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}
		end = stat.Size()
	}

	data := make([]byte, end-start)
	_, err = file.ReadAt(data, start)

	return data, err
}
