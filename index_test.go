package wally

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTest(t *testing.T) {
	idx := Idx{}
	idx.Records = []int32{1, 2, 3, 4, 5}
	idx.StartOffset = 5
	err := idx.Write("/tmp/test.idx")
	assert.Nil(t, err)
	newIdx := Idx{}
	err = newIdx.Read("/tmp/test.idx")
	assert.Nil(t, err)
	assert.Equal(t, idx.StartOffset, newIdx.StartOffset)
	for a := range newIdx.Records {
		assert.Equal(t, newIdx.Records[a], idx.Records[a])
	}
	os.Remove("/tmp/test.idx")
}

func TestRWMasterIndex(t *testing.T) {
	teardown()
	masterIdxName, idx1Name, _ := getFilenames()
	mi := &MasterIndex{Filename: masterIdxName}
	mi.Indices = append(mi.Indices, Index{Filename: idx1Name})
	err := WriteMasterIndex(mi)
	assert.Nil(t, err)

	mi, err = ReadMasterIndex(masterIdxName)
	assert.Nil(t, err)
	assert.NotNil(t, mi)
	assert.Equal(t, idx1Name, mi.Indices[0].Filename)
	teardown()
}

func TestIndex(t *testing.T) {
	teardown()
	masterIdxName, idx1Name, dat1Name := getFilenames()
	mi := &MasterIndex{Filename: masterIdxName}
	idx := Index{Filename: idx1Name, BlobFilename: dat1Name, StartOffset: 0}
	err := mi.WriteIndex(idx)
	newMi, err := ReadMasterIndex(masterIdxName)
	assert.Nil(t, err)
	assert.NotNil(t, newMi)
	newIdx := newMi.LastIndex()
	assert.NotNil(t, newIdx)
	assert.Equal(t, idx1Name, newIdx.Filename)
	assert.Equal(t, dat1Name, newIdx.BlobFilename)
	teardown()
}

func TestWrite(t *testing.T) {
	teardown()
	mi, idx := setup()
	idx, err := mi.Write(idx, []byte("Data1"))
	idx, err = mi.Write(idx, []byte("Data2"))
	idx, err = mi.Write(idx, []byte("Data3"))
	assert.Nil(t, err)
	assert.NotNil(t, idx)
	data, err := mi.Read(idx, 0)
	assert.Nil(t, err)
	assert.True(t, bytes.Equal([]byte("Data1"), data))
	data, err = mi.Read(idx, 1)
	assert.Nil(t, err)
	assert.True(t, bytes.Equal([]byte("Data2"), data))
	data, err = mi.Read(idx, 2)
	assert.Nil(t, err)
	assert.True(t, bytes.Equal([]byte("Data3"), data))
	teardown()
}

func setup() (*MasterIndex, Index) {
	m, i, d := getFilenames()
	mi := &MasterIndex{Filename: m}
	idx := Index{Filename: i, BlobFilename: d, StartOffset: 0}
	mi.WriteIndex(idx)
	WriteMasterIndex(mi)
	return mi, idx
}

func getFilenames() (string, string, string) {
	masterIdx, _ := ioutil.TempFile("", "testMasterIndex")
	masterIdxName := masterIdx.Name()
	idx1, _ := ioutil.TempFile("", "test1.idx")
	idx1Name := idx1.Name()
	dat1, _ := ioutil.TempFile("", "test1.dat")
	dat1Name := dat1.Name()
	return masterIdxName, idx1Name, dat1Name
}

func teardown() {
	m, i, d := getFilenames()
	os.Remove(m)
	os.Remove(i)
	os.Remove(d)
}
