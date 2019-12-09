package pbzlib

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"sync"

	"github.com/golang/protobuf/proto"
)

const MAGIC = "\x41\x42"
const T_FILE_DESCRIPTOR = 1
const T_DESCRIPTOR_NAME = 2
const T_MESSAGE = 3

// ---------------------------------------------------------------------------

type Writer struct {
	fhandle         *os.File
	gzhandle        *gzip.Writer
	last_descriptor string
}

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

func NewWriter(fname string, fdescr string) (*Writer, error) {
	w := new(Writer)
	err := w.open(fname, fdescr)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) open(fname string, fdescr string) error {
	// Read protobuf descriptor set
	descr, err := ioutil.ReadFile(fdescr)
	if err != nil {
		return err
	}

	w.fhandle, err = os.Create(fname)
	if err != nil {
		return err
	}

	w.gzhandle = gzip.NewWriter(w.fhandle)

	// Write magic header
	w.gzhandle.Write([]byte(MAGIC))

	// Write protobuf descriptor set
	writeTLV(w.gzhandle, T_FILE_DESCRIPTOR, descr)
	return nil
}

func (w *Writer) Write(msg proto.Message) error {
	// Write message type in case it is a new message type
	descriptor := proto.MessageName(msg)
	if descriptor != w.last_descriptor {
		buf := []byte(descriptor)
		writeTLV(w.gzhandle, T_DESCRIPTOR_NAME, buf)
		w.last_descriptor = descriptor
	}

	// Marshal message and writes it to file
	buf, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	writeTLV(w.gzhandle, T_MESSAGE, buf)
	return nil
}

func (w *Writer) Close() {
	w.gzhandle.Close()
	w.fhandle.Close()
}

func (w *Writer) Flush() {
	w.gzhandle.Flush()
}

func PBZWriter(fname string, fdescr string, messages chan proto.Message, wg *sync.WaitGroup, done chan bool) {
	defer wg.Done()

	w, err := NewWriter(fname, fdescr)
	if err != nil {
		panic(err)
	}
	defer w.Close()

L:
	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				break L
			}
			err = w.Write(msg)
			if err != nil {
				panic(err)
			}
		case <-done:
			break L
		}
	}
}

// ---------------------------------------------------------------------------

type Reader struct {
	fhandle  *os.File
	gzhandle *gzip.Reader
	rdr      *bufio.Reader
	nextType reflect.Type
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

func NewReader(path string) (*Reader, error) {
	r := new(Reader)
	err := r.open(path)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) open(path string) error {
	var err error
	r.fhandle, err = os.Open(path)
	if err != nil {
		return err
	}

	r.gzhandle, err = gzip.NewReader(r.fhandle)
	if err != nil {
		return err
	}

	r.rdr = bufio.NewReader(r.gzhandle)

	// Read magic and makes sure it's the correct one
	magic := make([]byte, 2)
	_, err = io.ReadFull(r.rdr, magic)
	if err != nil {
		return err
	}

	if bytes.Compare(magic, []byte(MAGIC)) != 0 {
		return errors.New("Invalid magic header")
	}
	return nil
}

func (r *Reader) Close() {
	r.gzhandle.Close()
	r.fhandle.Close()
}

func (r *Reader) Read() (proto.Message, error) {
	for {
		vtype, buf, err := readTLV(r.rdr)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		switch vtype {
		case T_FILE_DESCRIPTOR:
			proto.RegisterFile("r", buf)

		case T_DESCRIPTOR_NAME:
			r.nextType = proto.MessageType(string(buf))

		case T_MESSAGE:
			msg := reflect.New(r.nextType.Elem()).Interface().(proto.Message)
			err := proto.Unmarshal(buf, msg)
			if err != nil {
				return nil, errors.New("Unknown type")
			}
			return msg, nil

		default:
			return nil, errors.New("Unknown type")
		}
	}
	return nil, io.EOF
}

func PBZReader(path string, messages chan proto.Message, wg *sync.WaitGroup, done chan bool) {
	defer wg.Done()
	rdr, err := NewReader(path)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

L:
	for {
		select {
		case <-done:
			break L
		default:
		}

		msg, err := rdr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		messages <- msg
	}
	close(messages)
}
