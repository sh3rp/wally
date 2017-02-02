package wally

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/ioutil"
)

//
// index file structure
//
// bits 0-32    - marker 0xDEAD
// bits 33-64   - number of entries
// bits 65-96   - starting offset
// rest of bits - record locators
//

var MARKER int32 = 0xDEAD

type Index struct {
	StartOffset int32
	Records     []int32
}

// Write persists the index data to the file specified by filename
func (idx *Index) Write(filename string) error {
	buf := new(bytes.Buffer)

	// write MARKER

	err := binary.Write(buf, binary.LittleEndian, MARKER)
	if err != nil {
		return err
	}

	// write record count

	err = binary.Write(buf, binary.LittleEndian, int32(len(idx.Records)))
	if err != nil {
		return err
	}

	// write record starting offset

	err = binary.Write(buf, binary.LittleEndian, idx.StartOffset)
	if err != nil {
		return err
	}
	for _, r := range idx.Records {
		err = binary.Write(buf, binary.LittleEndian, r)
		if err != nil {
			return err
		}
	}
	return ioutil.WriteFile(filename, buf.Bytes(), 0600)
}

// Read reads the index data from the file specified by filename
func (idx *Index) Read(filename string) error {
	b, err := ioutil.ReadFile(filename)

	if err != nil {
		return err
	}

	buf := bytes.NewReader(b)

	// read marker and verify index

	var marker int32

	err = binary.Read(buf, binary.LittleEndian, &marker)

	if err != nil {
		return err
	}

	if marker != MARKER {
		return errors.New("Not a Wally index")
	}

	// read record count

	var numRecords int32
	err = binary.Read(buf, binary.LittleEndian, &numRecords)

	if err != nil {
		return err
	}

	idx.Records = make([]int32, numRecords)

	// read starting offset

	var offset int32
	err = binary.Read(buf, binary.LittleEndian, &offset)

	if err != nil {
		return err
	}

	idx.StartOffset = offset

	// read in record pointers

	for i := 0; i < len(idx.Records); i++ {
		var pos int32
		err = binary.Read(buf, binary.LittleEndian, &pos)
		idx.Records[i] = pos
	}

	return nil
}
