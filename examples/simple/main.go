package main

import (
	"fmt"
	"io"

	"github.com/fabgeyer/pbzlib-go"
	"github.com/fabgeyer/pbzlib-go/tests"
)

func main() {
	// -----------------------------------------------------
	// Writer example using NewWriter

	wrt, err := pbzlib.NewWriter("output.pbz", "../../tests/messages.descr")
	if err != nil {
		panic(err)
	}

	wrt.Write(&tests.Header{Version: 1})
	for i := int32(0); i < 10; i++ {
		wrt.Write(&tests.Object{Id: i})
	}
	wrt.Close()

	// -----------------------------------------------------
	// Reader example using NewReader

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
