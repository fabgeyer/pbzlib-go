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
	"github.com/schollz/progressbar/v3"
	"google.golang.org/protobuf/runtime/protoimpl"
)

// ---------------------------------------------------------------------------

type Writer struct {
	fhandle         *os.File
	gzhandle        *gzip.Writer
	last_descriptor string
}

func writeTLV(w io.Writer, vtype byte, buf []byte) error {
	// Write type of message
	_, err := w.Write([]byte{vtype})
	if err != nil {
		return err
	}

	// Write size of message as uvarint
	bufsz := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(bufsz, uint64(len(buf)))
	_, err = w.Write(bufsz[:n])
	if err != nil {
		return err
	}

	// Write message
	_, err = w.Write(buf)
	if err != nil {
		return err
	}
	return nil
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
	_, err = w.gzhandle.Write([]byte(MAGIC))
	if err != nil {
		return err
	}

	// Write protobuf descriptor set
	return writeTLV(w.gzhandle, T_FILE_DESCRIPTOR, descr)
}

func (w *Writer) WriteRaw(vtype byte, buf []byte) error {
	return writeTLV(w.gzhandle, vtype, buf)
}

func (w *Writer) Write(msg proto.Message) error {
	// Write message type in case it is a new message type
	descriptor := proto.MessageName(msg)
	if descriptor != w.last_descriptor {
		buf := []byte(descriptor)
		err := writeTLV(w.gzhandle, T_DESCRIPTOR_NAME, buf)
		if err != nil {
			return err
		}
		w.last_descriptor = descriptor
	}

	// Marshal message and writes it to file
	buf, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return writeTLV(w.gzhandle, T_MESSAGE, buf)
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
	err := r.open(path, false, "")
	if err != nil {
		return nil, err
	}
	return r, nil
}

func NewReaderWithProgressBar(path string, description string) (*Reader, error) {
	r := new(Reader)
	err := r.open(path, true, description)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) open(path string, withProgressBar bool, description string) error {
	var err error
	r.fhandle, err = os.Open(path)
	if err != nil {
		return err
	}

	if withProgressBar {
		stat, _ := os.Stat(path)
		bar := progressbar.DefaultBytes(stat.Size(), description)
		r.gzhandle, err = gzip.NewReader(io.TeeReader(r.fhandle, bar))
	} else {
		r.gzhandle, err = gzip.NewReader(r.fhandle)
	}
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

func (r *Reader) ReadRaw() (byte, []byte, error) {
	return readTLV(r.rdr)
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
			protoimpl.DescBuilder{RawDescriptor: buf}.Build()

		case T_DESCRIPTOR_NAME:
			r.nextType = proto.MessageType(string(buf))

		case T_MESSAGE:
			msg := reflect.New(r.nextType.Elem()).Interface().(proto.Message)
			err := proto.Unmarshal(buf, msg)
			if err != nil {
				return nil, errors.New("Unknown type")
			}
			return msg, nil

		case T_PROTOBUF_VERSION:
			continue

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
