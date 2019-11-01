package pbzlib

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"

	"github.com/golang/protobuf/proto"
)

const MAGIC = "\x41\x42"
const T_FILE_DESCRIPTOR = 1
const T_DESCRIPTOR_NAME = 2
const T_MESSAGE = 3

func writeTLV(w io.Writer, vtype byte, buf []byte) {
	// Write type of message
	w.Write([]byte{vtype})

	// Write size of message as uvarint
	bufsz := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(bufsz, uint64(len(buf)))
	w.Write(bufsz[:n])

	// Write message
	w.Write(buf)
}

func PBZWriter(fname string, fdescr string, messages chan proto.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	// Read protobuf descriptor set
	descr, err := ioutil.ReadFile(fdescr)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(fname)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	w := gzip.NewWriter(f)
	defer w.Close()

	// Write magic header
	w.Write([]byte(MAGIC))

	// Write protobuf descriptor set
	writeTLV(w, T_FILE_DESCRIPTOR, descr)

	last_descriptor := ""
	for {
		msg, ok := <-messages
		if !ok {
			break
		}

		// Write message type in case it is a new message type
		descriptor := proto.MessageName(msg)
		if descriptor != last_descriptor {
			buf := []byte(descriptor)
			writeTLV(w, T_DESCRIPTOR_NAME, buf)
			last_descriptor = descriptor
		}

		// Marshal message and writes it to file
		buf, err := proto.Marshal(msg)
		if err != nil {
			log.Fatal(err)
		}
		writeTLV(w, T_MESSAGE, buf)
	}
}

func readTLV(r *bufio.Reader) (byte, []byte, error) {
	// Read type
	vtype, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	// Read size
	size, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, nil, err
	}

	// Read value
	buf := make([]byte, size)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return 0, nil, err
	}
	return vtype, buf, nil
}

func PBZReader(path string, messages chan proto.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	gzrdr, err := gzip.NewReader(f)
	if err != nil {
		panic(err)
	}
	defer gzrdr.Close()
	rdr := bufio.NewReader(gzrdr)

	// Read magic and makes sure it's the correct one
	magic := make([]byte, 2)
	_, err = io.ReadFull(rdr, magic)
	if err != nil {
		panic(err)
	}

	if bytes.Compare(magic, []byte(MAGIC)) != 0 {
		panic(errors.New("Invalid magic header"))
	}

	var nextType reflect.Type
	for {
		vtype, buf, err := readTLV(bufio.NewReader(rdr))
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		switch vtype {
		case T_FILE_DESCRIPTOR:
			proto.RegisterFile("r", buf)

		case T_DESCRIPTOR_NAME:
			nextType = proto.MessageType(string(buf))

		case T_MESSAGE:
			msg := reflect.New(nextType.Elem()).Interface().(proto.Message)
			err := proto.Unmarshal(buf, msg)
			if err != nil {
				panic(err)
			}
			messages <- msg

		default:
			panic(errors.New("Unknown type"))
		}
	}
	close(messages)
}
