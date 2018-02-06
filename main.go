package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dgraph-io/badger"
	"github.com/tidwall/resp"
)

type BadgerDB struct {
	db *badger.DB
}

func NewKV(metaDir, valueDir string) *BadgerDB {
	opts := badger.DefaultOptions
	opts.Dir = metaDir
	opts.ValueDir = valueDir
    opts.SyncWrites = false

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return &BadgerDB{
		db: db,
	}
}

func (b *BadgerDB) Close() {
	b.db.Close()
}

func (b *BadgerDB) Get(conn *resp.Conn, args []resp.Value) bool {
	if len(args) != 2 {
		conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
	} else {
		key := args[1].Bytes()
		var val []byte

		err := b.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}

			val, err = item.Value()
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			fmt.Println(err)
		}

		if val == nil {
			conn.WriteNull()
		} else {
			conn.WriteBytes(val)
		}
	}
	return true
}

func (b *BadgerDB) Set(conn *resp.Conn, args []resp.Value) bool {
	if len(args) != 3 {
		conn.WriteError(errors.New("ERR wrong number of arguments for 'set' command"))
	} else {
		key := args[1].Bytes()
		val := args[2].Bytes()

		err := b.db.Update(func(txn *badger.Txn) error {
			err := txn.Set(key, val)
			return err
		})

		if err != nil {
			log.Print(err)
		}

		conn.WriteBytes(key)
	}
	return true
}

func (b *BadgerDB) Ping(conn *resp.Conn, args []resp.Value) bool {
	if len(args) == 1 {
		conn.WriteSimpleString("PONG")
	} else if len(args) == 2 {
		conn.WriteSimpleString(fmt.Sprintf("PONG %s", args[1].String()))
	} else {
		conn.WriteError(errors.New("ERR wrong number of arguments for 'ping' command"))
	}
	return true
}

var addr = flag.String("addr", ":16379", "listening address")
var metaDir = flag.String("value", "db/meta", "metadata directory for kvs storage")
var valueDir = flag.String("meta", "db/data", "data directory for kvs storage")

func main() {
	flag.Parse()

	os.MkdirAll(*metaDir, 0774)
	os.MkdirAll(*valueDir, 0774)

	kv := NewKV(*metaDir, *valueDir)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)

	defer func() {
		log.Println("closing kvs")
		kv.Close()
		os.Exit(0)
	}()

	s := resp.NewServer()

	s.HandleFunc("set", kv.Set)
	s.HandleFunc("get", kv.Get)
	s.HandleFunc("ping", kv.Ping)

	go func() {
		<-c
		log.Println("closing kvs")
		kv.Close()
		os.Exit(0)
	}()

	log.Printf("server listenting on %v\n", *addr)
	if err := s.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}
