package main

import (
	"fmt"
	"sync"

	"github.com/fabgeyer/pbzlib-go/tests"
	"github.com/fabgeyer/pbzlib-go/pbzlib"
	"github.com/golang/protobuf/proto"
)

func main() {
	// -----------------------------------------------------
	// Writer example

	var wg sync.WaitGroup
	wmessages := make(chan proto.Message)
	wg.Add(1)
	go pbzlib.PBZWriter("output.pbz", "tests/messages.descr", wmessages, &wg)

	wmessages <- &tests.Header{Version: 1}
	for i := int32(0); i < 10; i++ {
		wmessages <- &tests.Object{Id: i}
	}
	close(wmessages)
	wg.Wait()

	// -----------------------------------------------------
	// Reader example

	rmessages := make(chan proto.Message)
	wg.Add(1)
	go pbzlib.PBZReader("output.pbz", rmessages, &wg)
	for {
		msg, ok := <-rmessages
		if !ok {
			break
		}
		fmt.Println(msg)
	}
	wg.Wait()
}
