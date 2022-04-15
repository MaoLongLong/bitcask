// Copyright 2022 MaoLongLong. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrInvalidEntry = errors.New("invalid entry")
	ErrNotExist     = errors.New("key does not exists")
)

const (
	_logFile   = "bitcask.log"
	_mergeFile = "bitcask.tmp.log"
)

const (
	_hdrSize       = 9
	_keySizeOffset = 1
	_valSizeOffset = 5
)

const (
	_ uint8 = iota
	_opPut
	_opDelete
)

var bufPool = &sync.Pool{New: func() any { return bytes.NewBuffer(nil) }}

type DB struct {
	indexes map[string]int64
	dir     string
	file    *os.File
	off     int64
	mu      sync.RWMutex
}

func Open(dir string) (*DB, error) {
	dirExists, err := exists(dir)
	if err != nil {
		return nil, err
	}
	if !dirExists {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	}

	file, err := openLogFile(filepath.Join(dir, _logFile))
	if err != nil {
		return nil, err
	}

	db := &DB{
		indexes: make(map[string]int64),
		dir:     dir,
		file:    file,
	}

	if err := db.loadIndexes(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (db *DB) Close() error {
	return db.file.Close()
}

func (db *DB) DropAll() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.file.Close()
	os.Remove(db.file.Name())
	db.off = 0

	// reopen empty log file
	var err error
	db.file, err = openLogFile(db.file.Name())
	if err != nil {
		return err
	}

	db.indexes = make(map[string]int64)
	return nil
}

func (db *DB) Put(key, val []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	e := &entry{op: _opPut, key: key, val: val}
	err := writeEntry(db.file, e)
	if err != nil {
		return err
	}

	db.indexes[string(key)] = db.off
	db.off += e.size()

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	offset, ok := db.indexes[string(key)]
	if !ok {
		return nil, ErrNotExist
	}

	e, err := readEntry(db.file, offset)
	if err != nil {
		return nil, err
	}

	return e.val, nil
}

func (db *DB) ForEach(fn func(key, val []byte) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, i := range db.indexes {
		e, err := readEntry(db.file, i)
		if err != nil {
			return err
		}
		if err := fn(e.key, e.val); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	keyStr := string(key)

	_, ok := db.indexes[keyStr]
	if !ok {
		return ErrNotExist
	}

	e := &entry{op: _opDelete, key: key, val: nil}
	err := writeEntry(db.file, e)
	if err != nil {
		return err
	}

	db.off += e.size()
	delete(db.indexes, keyStr)
	return nil
}

func (db *DB) Reclaim() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.off == 0 {
		return nil
	}

	var (
		entries []*entry
		off     int64
	)

	for {
		e, err := readEntry(db.file, off)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if i, ok := db.indexes[string(e.key)]; ok && i == off {
			entries = append(entries, e)
		}
		off += e.size()
	}

	if len(entries) > 0 {
		tmp, err := os.Create(filepath.Join(db.dir, _mergeFile))
		if err != nil {
			return err
		}

		db.off = 0
		for _, e := range entries {
			err := writeEntry(tmp, e)
			if err != nil {
				tmp.Close()
				os.Remove(tmp.Name())
				return err
			}

			db.indexes[string(e.key)] = db.off
			db.off += e.size()
		}

		db.file.Close()
		tmp.Close()

		os.Remove(db.file.Name())
		if err := os.Rename(tmp.Name(), db.file.Name()); err != nil {
			return err
		}

		db.file, err = openLogFile(db.file.Name())
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) loadIndexes() error {
	for {
		e, err := readEntry(db.file, db.off)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		key := string(e.key)

		switch e.op {
		case _opPut:
			db.indexes[key] = db.off
		case _opDelete:
			delete(db.indexes, key)
		}

		db.off += e.size()
	}
	return nil
}

type entry struct {
	op  uint8
	key []byte
	val []byte
}

func (e *entry) size() int64 {
	return _hdrSize + int64(len(e.key)) + int64(len(e.val))
}

func readEntry(r io.ReaderAt, off int64) (*entry, error) {
	hdr := make([]byte, _hdrSize)
	if _, err := r.ReadAt(hdr, off); err != nil {
		return nil, err
	}

	e := new(entry)
	switch op := hdr[0]; op {
	case _opPut, _opDelete:
		e.op = op
	default:
		return nil, ErrInvalidEntry
	}

	keySize := int64(binary.BigEndian.Uint32(hdr[_keySizeOffset:]))
	valSize := int64(binary.BigEndian.Uint32(hdr[_valSizeOffset:]))

	off += _hdrSize
	if keySize > 0 {
		key := make([]byte, keySize)
		if _, err := r.ReadAt(key, off); err != nil {
			return nil, err
		}
		e.key = key
	}

	off += keySize
	if valSize > 0 {
		val := make([]byte, valSize)
		if _, err := r.ReadAt(val, off); err != nil {
			return nil, err
		}
		e.val = val
	}

	return e, nil
}

func writeEntry(w io.Writer, e *entry) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	keySize, valSize := len(e.key), len(e.val)

	hdr := make([]byte, _hdrSize)
	hdr[0] = e.op
	binary.BigEndian.PutUint32(hdr[_keySizeOffset:], uint32(keySize))
	binary.BigEndian.PutUint32(hdr[_valSizeOffset:], uint32(valSize))
	buf.Write(hdr)

	buf.Write(e.key)
	buf.Write(e.val)

	_, err := w.Write(buf.Bytes())
	return err
}

func openLogFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}
