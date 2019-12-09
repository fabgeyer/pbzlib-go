# Library for serializing protobuf objects - Go version

This library is used for simplifying the serialization and deserialization of [protocol buffer](https://developers.google.com/protocol-buffers/) objects to/from files.
The main use-case is to save and read a large collection of objects of the same type.
Each file contains a header with the description of the protocol buffer, meaning that no compilation of `.proto` description file is required before reading a `pbz` file.


## Installation

```
$ go get github.com/fabgeyer/pbzlib-go/pbzlib
```


## Example

Reading a `pbz` file:

```go
package main

import (
	"fmt"
	"sync"

	"github.com/fabgeyer/pbzlib-go/pbzlib"
	"github.com/golang/protobuf/proto"
)

func main() {
	var wg sync.WaitGroup
	messages := make(chan proto.Message)
	wg.Add(1)
	go pbzlib.PBZReader("output.pbz", messages, &wg)
	for {
		msg, ok := <-messages
		if !ok {
			break
		}
		fmt.Println(msg)
	}
	wg.Wait()
}
```


## Versions in other languages

- [Python version](https://github.com/fabgeyer/pbzlib-py)
- [C/C++ version](https://github.com/fabgeyer/pbzlib-c-cpp)
- [Java version](https://github.com/fabgeyer/pbzlib-java)
