# Wally
A write-ahead-log library written in Go.

# Quick-start
To install, simple run `go get -u github.com/sh3rp/wally`.

Example:
```
wally := NewWally("/tmp/logdir","WallyTest")
w.Write([]byte("test data0"))
w.Write([]byte("test data1"))
w.Write([]byte("test data2"))

data, err := w.Next()
fmt.Println(string(data))
data, err = w.Peek(2)
fmt.Println(string(data))
```
