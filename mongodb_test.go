package db

import (
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"github.com/strikesecurity/strikememongo"
)

func TestMongoDBNewMongoDB(t *testing.T) {
	// Start an in-memory MongoDB server
	options := &strikememongo.Options{MongoVersion: "4.0.5"}
	mongoServer, err := strikememongo.StartWithOptions(options)
	require.Nil(t, err)
	defer mongoServer.Stop()

	// Get MongoDB URI
	uri := mongoServer.URI()
	spew.Dump(uri)

	// Generate a random database name
	name := fmt.Sprintf("test_%x", randStr(12))

	// Test we can open the db
	wr1, err := NewMongoDB(name, uri)
	require.Nil(t, err)
	defer wr1.Close()

	// Test we can open the db again (MongoDB allows multiple connections)
	wr2, err := NewMongoDB(name, uri)
	require.Nil(t, err)
	defer wr2.Close()
}

func BenchmarkMongoDBRandomReadsWrites(b *testing.B) {
	// Start an in-memory MongoDB server
	options := &strikememongo.Options{MongoVersion: "4.0.5"}
	mongoServer, err := strikememongo.StartWithOptions(options)
	require.Nil(b, err)
	defer mongoServer.Stop()

	// Get MongoDB URI
	uri := mongoServer.URI()
	// Generate a random database name
	name := fmt.Sprintf("test_%x", randStr(12))

	db, err := NewMongoDB(name, uri)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	benchmarkRandomReadsWrites(b, db)
}
