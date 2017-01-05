package wally

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRWMasterIndex(t *testing.T) {
	teardown()
	mi := &MasterIndex{Filename: "/tmp/testMasterIndex"}
	mi.Indices = append(mi.Indices, Index{Filename: "/tmp/index1.idx"})
	err := WriteMasterIndex(mi)
	assert.Nil(t, err)

	mi, err = ReadMasterIndex("/tmp/testMasterIndex")
	assert.Nil(t, err)
	assert.NotNil(t, mi)
	assert.Equal(t, "/tmp/index1.idx", mi.Indices[0].Filename)
	teardown()
}

func TestIndex(t *testing.T) {
	teardown()
	mi := &MasterIndex{Filename: "/tmp/testMasterIndex"}
	idx := Index{Filename: "/tmp/test1.idx", BlobFilename: "/tmp/test1.dat", StartOffset: 0}
	err := mi.WriteIndex(idx)
	newMi, err := ReadMasterIndex("/tmp/testMasterIndex")
	assert.Nil(t, err)
	assert.NotNil(t, newMi)
	newIdx := newMi.LastIndex()
	assert.NotNil(t, newIdx)
	assert.Equal(t, "/tmp/test1.idx", newIdx.Filename)
	assert.Equal(t, "/tmp/test1.dat", newIdx.BlobFilename)
	teardown()
}

func TestWrite(t *testing.T) {
	teardown()
	mi, idx := setup()
	idx, err := mi.Write(idx, []byte("This is test data"))
	assert.Nil(t, err)
	assert.NotNil(t, idx)
	data, err := mi.Read(idx, 0)
	assert.Nil(t, err)
	assert.True(t, bytes.Equal([]byte("This is test data"), data))
	teardown()
}

func setup() (*MasterIndex, Index) {
	mi := &MasterIndex{Filename: "/tmp/testMasterIndex"}
	idx := Index{Filename: "/tmp/test1.idx", BlobFilename: "/tmp/test1.dat", StartOffset: 0}
	mi.WriteIndex(idx)
	WriteMasterIndex(mi)
	return mi, idx
}

func teardown() {
	os.Remove("/tmp/testMasterIndex")
	os.Remove("/tmp/test1.idx")
	os.Remove("/tmp/test1.dat")
}
