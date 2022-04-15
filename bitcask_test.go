// Copyright 2022 MaoLongLong. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitcask(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "*")
	require.NoError(t, err)

	db, err := Open(dir)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Get([]byte("hello"))
	require.ErrorIs(t, err, ErrNotExist)

	err = db.Put([]byte("hello"), []byte("world"))
	require.NoError(t, err)

	val, err := db.Get([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, []byte("world"), val)

	db.Close()
	db, err = Open(dir)
	require.NoError(t, err)

	require.NoError(t, db.Reclaim())

	val, err = db.Get([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, []byte("world"), val)

	require.NoError(t, db.Delete([]byte("hello")))
	require.ErrorIs(t, db.Delete([]byte("hello")), ErrNotExist)

	_, err = db.Get([]byte("hello"))
	require.ErrorIs(t, err, ErrNotExist)
}
