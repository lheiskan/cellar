package cellar

import (
	"encoding/binary"
	"log"

	"github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	"github.com/lheiskan/lex-go/tuple"
	"github.com/lheiskan/mdb"
	"github.com/pkg/errors"
)

const (
	ChunkTable          byte = 1
	MetaTable           byte = 2
	BufferTable         byte = 3
	CellarTable         byte = 4
	UserIndexTable      byte = 5
	UserCheckpointTable byte = 6
)

func badgerPutUserCheckpoint(tx *mdb.Tx, name string, pos int64) error {
	key := mdb.CreateKey(UserCheckpointTable, name)

	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(pos))
	err := tx.Tx.Set(key, value)
	//value, err := tx.PutReserve(key, 8)
	if err != nil {
		return errors.Wrap(err, "PutReserve")
	}
	//binary.LittleEndian.PutUint64(value, uint64(pos))
	return nil
}

func badgerGetUserCheckpoint(tx *mdb.Tx, name string) (int64, error) {

	key := mdb.CreateKey(UserCheckpointTable, name)
	value, err := tx.Get(key)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, errors.Wrap(err, "Get")
	}
	return int64(binary.LittleEndian.Uint64(value)), nil
}

func badgerAddChunk(tx *mdb.Tx, chunkStartPos int64, dto *ChunkDto) error {
	key := mdb.CreateKey(ChunkTable, chunkStartPos)

	if err := tx.PutProto(key, dto); err != nil {
		return errors.Wrap(err, "PutProto")
	}

	log.Printf("Added chunk %s with %d records and %d bytes (%d compressed)", dto.FileName, dto.Records, dto.UncompressedByteSize, dto.CompressedDiskSize)
	return nil
}

func badgerListChunks(tx *mdb.Tx) ([]*ChunkDto, error) {

	prefix := mdb.CreateKey(ChunkTable)

	var chunks []*ChunkDto
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 100
	it := tx.Tx.NewIterator(opts)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		var chunk = &ChunkDto{}
		var v []byte
		v, err := item.ValueCopy(v)
		if err != nil {
			return nil, errors.Wrap(err, "item.ValueCopy")
		}
		if err := proto.Unmarshal(v, chunk); err != nil {
			return nil, errors.Wrapf(err, "Unmarshal %x at %x", v, k)
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

func badgerPutBuffer(tx *mdb.Tx, dto *BufferDto) error {
	tpl := tuple.Tuple([]tuple.Element{BufferTable})

	key := tpl.Pack()
	var val []byte
	var err error

	if val, err = proto.Marshal(dto); err != nil {
		return errors.Wrap(err, "Marshal")
	}
	if err = tx.Put(key, val); err != nil {
		return errors.Wrap(err, "tx.Put")
	}
	return nil
}

func badgerGetBuffer(tx *mdb.Tx) (*BufferDto, error) {

	tpl := tuple.Tuple([]tuple.Element{BufferTable})
	key := tpl.Pack()
	var data []byte
	var err error

	if data, err = tx.Get(key); err != nil {
		return nil, errors.Wrap(err, "tx.Get")
	}
	if data == nil {
		return nil, nil
	}
	dto := &BufferDto{}
	if err = proto.Unmarshal(data, dto); err != nil {
		return nil, errors.Wrap(err, "Unmarshal")
	}
	return dto, nil
}

func badgerIndexPosition(tx *mdb.Tx, stream string, k uint64, pos int64) error {
	tpl := tuple.Tuple([]tuple.Element{MetaTable, stream, k})
	key := tpl.Pack()
	var err error

	buf := make([]byte, binary.MaxVarintLen64)

	n := binary.PutVarint(buf, pos)
	if err = tx.Put(key, buf[0:n]); err != nil {
		return errors.Wrap(err, "tx.Put")
	}
	return nil
}

func badgerLookupPosition(tx *mdb.Tx, stream string, k uint64) (int64, error) {

	tpl := tuple.Tuple([]tuple.Element{MetaTable, stream, k})
	key := tpl.Pack()
	var err error

	var val []byte
	if val, err = tx.Get(key); err != nil {
		return 0, errors.Wrap(err, "tx.Get")
	}
	var pos int64

	pos, _ = binary.Varint(val)
	return pos, nil
}

func badgerSetCellarMeta(tx *mdb.Tx, m *MetaDto) error {
	key := mdb.CreateKey(CellarTable)
	return tx.PutProto(key, m)
}

func badgerGetCellarMeta(tx *mdb.Tx) (*MetaDto, error) {

	key := mdb.CreateKey(CellarTable)
	dto := &MetaDto{}
	var err error

	if err = tx.ReadProto(key, dto); err != nil {
		return nil, errors.Wrap(err, "ReadProto")
	}
	return dto, nil

}
