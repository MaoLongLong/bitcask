# bitcask

A tiny [bitcask](https://riak.com/assets/bitcask-intro.pdf) implementation.

```go
package main

import (
	"fmt"
	"log"

	"go.chensl.me/bitcask"
)

func main() {
	db, err := bitcask.Open("./data")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		log.Fatal(err)
	}

	val, err := db.Get([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(val)
}
```
