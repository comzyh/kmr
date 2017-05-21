package records

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Record struct {
	Key   []byte
	Value []byte
}

type ByKey []Record

func (r ByKey) Len() int {
	return len(r)
}

func (r ByKey) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r ByKey) Less(i, j int) bool {
	return bytes.Compare(r[i].Key, r[j].Key) == -1
}

func ReadRecord(reader io.Reader) (record Record, err error) {
	var keySize int32
	var valueSize int32
	err = binary.Read(reader, binary.BigEndian, &keySize)
	if err != nil {
		return
	}
	key := make([]byte, keySize)
	_, err = io.ReadFull(reader, key)
	if err != nil {
		return
	}
	//read Value
	err = binary.Read(reader, binary.BigEndian, &valueSize)
	if err != nil {
		return
	}
	value := make([]byte, valueSize)
	_, err = io.ReadFull(reader, value)
	if err != nil {
		return
	}
	return Record{Key: key, Value: value}, nil
}
func WriteRecord(writer io.Writer, record Record) (err error) {
	err = binary.Write(writer, binary.BigEndian, int32(len(record.Key)))
	if err != nil {
		return
	}
	_, err = writer.Write(record.Key)
	if err != nil {
		return
	}
	err = binary.Write(writer, binary.BigEndian, int32(len(record.Value)))
	if err != nil {
		return
	}
	_, err = writer.Write(record.Value)
	if err != nil {
		return
	}
	return nil
}
