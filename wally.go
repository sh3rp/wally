package wally

import "os"

type Wally struct {
	Name    string
	BaseDir string
	Index   *MasterIndex
}

func NewWally(dir string, name string) *Wally {
	if _, err := os.Stat(dir); err != nil {
		os.MkdirAll(dir, 0600)
	}
	path := dir + "/" + name + ".mdx"
	masterIndex := &MasterIndex{Filename: path}
	return &Wally{Index: masterIndex, Name: name, BaseDir: dir}
}

func (w *Wally) Write(data []byte) (int, error) {
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
