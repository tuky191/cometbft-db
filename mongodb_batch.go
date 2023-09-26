package db

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBBatch struct {
	collection     *mongo.Collection
	syncCollection *mongo.Collection // For synchronous operations
	ops            []mongo.WriteModel
	closed         bool
}

var _ Batch = (*MongoDBBatch)(nil)

func newMongoDBBatch(collection *mongo.Collection, syncCollection *mongo.Collection) *MongoDBBatch {
	return &MongoDBBatch{
		collection:     collection,
		syncCollection: syncCollection,
		ops:            []mongo.WriteModel{},
		closed:         false,
	}
}

// Set implements Batch.
func (b *MongoDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	if b.closed {
		return fmt.Errorf("batch has already been closed")
	}

	// b.ops = append(b.ops, mongo.NewInsertOneModel().SetDocument(bson.M{"key": key, "value": value}))
	b.ops = append(b.ops, mongo.NewUpdateOneModel().
		SetUpsert(true).
		SetFilter(bson.M{"key": key}).
		SetUpdate(bson.M{"$set": bson.M{"value": value, "keyString": string(key)}}))
	return nil
}

// Delete implements Batch.
func (b *MongoDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	if b.closed {
		return fmt.Errorf("batch has already been closed")
	}

	b.ops = append(b.ops, mongo.NewDeleteOneModel().SetFilter(bson.M{"key": key}))
	return nil
}

// Write implements Batch.
func (b *MongoDBBatch) Write() error {
	return b.write(false)
}

// WriteSync implements Batch.
func (b *MongoDBBatch) WriteSync() error {
	return b.write(true)
}

func (b *MongoDBBatch) write(sync bool) error {
	if b.closed {
		return fmt.Errorf("batch has already been closed")
	}

	var targetCollection *mongo.Collection
	if sync {
		targetCollection = b.syncCollection
	} else {
		targetCollection = b.collection
	}
	writeOptions := &options.BulkWriteOptions{}
	writeOptions.SetOrdered(true)

	if len(b.ops) != 0 {
		_, err := targetCollection.BulkWrite(context.Background(), b.ops, writeOptions)
		if err != nil {
			return err
		}
	}
	b.closed = true
	return b.Close()
}

// Close implements Batch.
func (b *MongoDBBatch) Close() error {
	b.ops = nil
	b.closed = true
	return nil
}
