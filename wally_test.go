package wally

import (
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWallyReadWrite(t *testing.T) {
	config := DefaultConfig()
	config.BaseDir = getDir()
	config.BaseName = "test"
	w := NewWally(config)
	w.Write([]byte("testdata1"))
	w.Write([]byte("testdata2"))
	w.Write([]byte("testdata3"))
	p1, err := w.Peek(0)
	p2, err := w.Peek(1)
	p3, err := w.Peek(2)
	assert.Nil(t, err)
	assert.Equal(t, p1, []byte("testdata1"))
	assert.Equal(t, p2, []byte("testdata2"))
	assert.Equal(t, p3, []byte("testdata3"))
}

func TestWallyRandReadWrite(t *testing.T) {
	config := DefaultConfig()
	config.BaseDir = getDir()
	config.BaseName = "test"
	w := NewWally(config)

	for i := 0; i < 1000; i++ {
		w.Write([]byte("testdata" + strconv.Itoa(i)))
	}
	p, err := w.Peek(132)
	assert.Nil(t, err)
	assert.Equal(t, p, []byte("testdata132"))
	p, err = w.Next()
	assert.Nil(t, err)
	assert.Equal(t, p, []byte("testdata0"))
}

func TestWallyMaxDataSize(t *testing.T) {
	config := DefaultConfig()
	config.BaseDir = getDir()
	config.BaseName = "test"
	w := NewWally(config)

	goodData := make([]byte, w.MaxDataSize)
	goodNumWritten, err := w.Write(goodData)
	assert.Equal(t, 1, goodNumWritten)
	assert.Nil(t, err)

	data := make([]byte, w.MaxDataSize+1)
	numWritten, err := w.Write(data)
	assert.Equal(t, 0, numWritten)
	assert.NotNil(t, err)
}

func getDir() string {
	dir, _ := ioutil.TempDir("", "tmp")
	return dir
}
