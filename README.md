# Library for serializing protobuf objects - Go version

This library is used for simplifying the serialization and deserialization of [protocol buffer](https://developers.google.com/protocol-buffers/) objects to/from files.
The main use-case is to save and read a large collection of objects of the same type.
Each file contains a header with the description of the protocol buffer, meaning that no compilation of `.proto` description file is required before reading a `pbz` file.


## Example

Reading a `pbz` file:

```go
package main

import (
	"fmt"
	"io"

	"github.com/fabgeyer/pbzlib-go"
)

func main() {
	rdr, _ := pbzlib.NewReader("output.pbz")
	defer rdr.Close()
	for {
		msg, err := rdr.Read()
		if err == io.EOF {
			break
		}
		fmt.Println(msg)
	}
}
```


## Versions in other languages

- [Python version](https://github.com/fabgeyer/pbzlib-py)
- [C/C++ version](https://github.com/fabgeyer/pbzlib-c-cpp)
- [Java version](https://github.com/fabgeyer/pbzlib-java)
