package cellar

import (
	"encoding/binary"
	fmt "fmt"
	"log"
	"os"
	"path"

	"github.com/abdullin/lex-go/subspace"
	"github.com/lheiskan/mdb"
	"github.com/pkg/errors"
)

type Writer struct {
	db            *mdb.DB
	b             *Buffer
	maxKeySize    int64
	maxValSize    int64
	folder        string
	maxBufferSize int64
	key           []byte
	encodingBuf   []byte
	subspace      subspace.Subspace
}

// NewWriter
func NewWriter(DB *mdb.DB, folder string, maxBufferSize int64, key []byte, subspace subspace.Subspace) (*Writer, error) {
	ensureFolder(folder)

	var meta *MetaDto
	var b *Buffer

	err := DB.Update(func(tx *mdb.Tx) error {
		var err error

		var dto *BufferDto
		if dto, err = badgerGetBuffer(subspace, tx); err != nil {
			return errors.Wrap(err, "badgerGetBuffer")
		}

		if dto == nil {
			if b, err = createBuffer(subspace, tx, 0, maxBufferSize, folder); err != nil {
				return errors.Wrap(err, "SetNewBuffer")
			}
			return nil

		} else if b, err = openBuffer(dto, folder); err != nil {
			return errors.Wrap(err, "openBuffer")
		}

		if meta, err = badgerGetCellarMeta(subspace, tx); err != nil {
			return errors.Wrap(err, "badgerGetCellarMeta")
		}
		return err
	})

	if err != nil {
		return nil, errors.Wrap(err, "Update")
	}

	wr := &Writer{
		folder:        folder,
		maxBufferSize: maxBufferSize,
		key:           key,
		encodingBuf:   make([]byte, binary.MaxVarintLen64),
		db:            DB,
		b:             b,
	}

	if meta != nil {
		wr.maxKeySize = meta.MaxKeySize
		wr.maxValSize = meta.MaxValSize
	}

	return wr, nil

}

func (w *Writer) VolatilePos() int64 {
	if w.b != nil {
		log.Println("Volatile position", w.b.startPos, w.b.pos)
		return w.b.startPos + w.b.pos
	}
	log.Println("Volatile position is 0")
	return 0
}

func (w *Writer) Append(data []byte) (pos int64, err error) {

	dataLen := int64(len(data))
	n := binary.PutVarint(w.encodingBuf, dataLen)

	totalSize := n + len(data)

	if !w.b.fits(int64(totalSize)) {
		if w.b.pos == 0 {
			log.Println("Value exceeds max size. Writing value into the file anyway")
		} else {
			log.Println("Buffer max size reached, sealing buffer", w.b.pos, totalSize, w.b.maxBytes)
			if err = w.SealTheBuffer(); err != nil {
				return 0, errors.Wrap(err, "SealTheBuffer")
			}
		}
	}

	if err = w.b.writeBytes(w.encodingBuf[0:n]); err != nil {
		return 0, errors.Wrap(err, "write len prefix")
	}
	if err = w.b.writeBytes(data); err != nil {
		return 0, errors.Wrap(err, "write body")
	}

	w.b.endRecord()

	// update statistics
	if dataLen > w.maxValSize {
		w.maxValSize = dataLen
	}

	pos = w.b.startPos + w.b.pos

	return pos, nil
}

func createBuffer(subspace subspace.Subspace, tx *mdb.Tx, startPos int64, maxSize int64, folder string) (*Buffer, error) {
	name := fmt.Sprintf("%012d", startPos)
	dto := &BufferDto{
		Pos:      0,
		StartPos: startPos,
		MaxBytes: maxSize,
		Records:  0,
		FileName: name,
	}
	var err error
	var buf *Buffer

	if buf, err = openBuffer(dto, folder); err != nil {
		return nil, errors.Wrapf(err, "openBuffer %s", folder)
	}

	if err = badgerPutBuffer(subspace, tx, dto); err != nil {
		return nil, errors.Wrap(err, "badgerPutBuffer")
	}
	return buf, nil

}

func (w *Writer) SealTheBuffer() error {

	var err error

	oldBuffer := w.b
	var newBuffer *Buffer

	if err = oldBuffer.flush(); err != nil {
		return errors.Wrap(err, "buffer.Flush")
	}

	var dto *ChunkDto

	if dto, err = oldBuffer.compress(w.key); err != nil {
		return errors.Wrap(err, "compress")
	}

	newStartPos := dto.StartPos + dto.UncompressedByteSize

	err = w.db.Update(func(tx *mdb.Tx) error {

		if err = badgerAddChunk(w.subspace, tx, dto.StartPos, dto); err != nil {
			return errors.Wrap(err, "badgerAddChunk")
		}

		if newBuffer, err = createBuffer(w.subspace, tx, newStartPos, w.maxBufferSize, w.folder); err != nil {
			return errors.Wrap(err, "createBuffer")
		}
		return nil

	})

	if err != nil {
		return errors.Wrap(err, "w.db.Update")
	}

	w.b = newBuffer

	oldBufferPath := path.Join(w.folder, oldBuffer.fileName)

	if err = os.Remove(oldBufferPath); err != nil {
		log.Printf("Can't remove old buffer %s: %s", oldBufferPath, err)
	}
	return nil

}

// Close disposes all resources
func (w *Writer) Close() error {

	// TODO: flush, checkpoint and close current buffer
	// NOTE: db is shared so no close per writer
	//return w.db.Close()
	return nil
}

// ReadDB allows to execute read transaction against
// the meta database
func (w *Writer) ReadDB(op mdb.TxOp) error {
	return w.db.Read(op)
}

// Write DB allows to execute write transaction against
// the meta database
func (w *Writer) UpdateDB(op mdb.TxOp) error {
	return w.db.Update(op)
}

func (w *Writer) PutUserCheckpoint(name string, pos int64) error {
	return w.db.Update(func(tx *mdb.Tx) error {
		return badgerPutUserCheckpoint(w.subspace, tx, name, pos)
	})
}

func (w *Writer) GetUserCheckpoint(name string) (int64, error) {

	var pos int64
	err := w.db.Read(func(tx *mdb.Tx) error {
		p, e := badgerGetUserCheckpoint(w.subspace, tx, name)
		if e != nil {
			return e
		}
		pos = p
		return nil
	})
	if err != nil {
		return 0, err
	}
	return pos, nil
}

func (w *Writer) Checkpoint() (int64, error) {
	w.b.flush()

	var err error

	dto := w.b.getState()

	current := dto.StartPos + dto.Pos

	err = w.db.Update(func(tx *mdb.Tx) error {
		var err error

		if err = badgerPutBuffer(w.subspace, tx, dto); err != nil {
			return errors.Wrap(err, "badgerPutBuffer")
		}

		meta := &MetaDto{
			MaxKeySize: w.maxKeySize,
			MaxValSize: w.maxValSize,
		}

		if err = badgerSetCellarMeta(w.subspace, tx, meta); err != nil {
			return errors.Wrap(err, "badgerSetCellarMeta")
		}
		return nil

	})

	if err != nil {
		return 0, errors.Wrap(err, "txn.Update")
	}

	return current, nil

}
