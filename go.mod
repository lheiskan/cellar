module github.com/abdullin/cellar

require (
	github.com/abdullin/lex-go v0.0.0-20170809071836-51ee1bbe34a4
	github.com/abdullin/mdb v0.0.0-20171224093530-b63d30c6dad8
	github.com/bmatsuo/lmdb-go v1.8.0
	github.com/dgraph-io/badger v1.6.0
	github.com/golang/protobuf v1.3.2
	github.com/lheiskan/lex-go v0.0.0-20170809071836-51ee1bbe34a4
	github.com/lheiskan/mdb v0.0.0-20171224093530-b63d30c6dad8
	github.com/pierrec/lz4 v0.0.0-20181005164709-635575b42742
	github.com/pierrec/xxHash v0.1.1 // indirect
	github.com/pkg/errors v0.8.1
)

replace github.com/lheiskan/mdb => ../mdb

replace github.com/lheiskan/lex-go => ../lex-go

replace github.com/bmatsuo/lmdb-go => ../lmdb-go
