module github.com/tuky191/cometbft-db

go 1.19

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/dgraph-io/badger/v2 v2.2007.4
	github.com/gogo/protobuf v1.3.2
	github.com/google/btree v1.1.2
	github.com/jmhodges/levigo v1.0.0
	github.com/linxGnu/grocksdb v1.7.16
	github.com/stretchr/testify v1.8.2
	github.com/strikesecurity/strikememongo v0.2.4
	github.com/syndtr/goleveldb v1.0.1-0.20200815110645-5c35d600f0ca
	go.etcd.io/bbolt v1.3.7
	go.mongodb.org/mongo-driver v1.12.1
	google.golang.org/grpc v1.53.0
)

require (
	github.com/acobaugh/osrelease v0.0.0-20181218015638-a93a0a55a249 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/dgraph-io/ristretto v0.0.3-0.20200630154024-f66de99634de // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/montanaflynn/stats v0.0.0-20171201202039-1bf9dbcd8cbe // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/afero v1.5.1 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	golang.org/x/crypto v0.0.0-20220622213112-05595931fe9d // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/strikesecurity/strikememongo => github.com/ariefdarmawan/strikememongo v0.0.0-20231010170346-44855ec27769

// Breaking changes were released with the wrong tag (use v0.6.6 or later).
retract v0.6.5
