package db

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func init() {
	dbCreator := NewMongoDB
	registerDBCreator(MongoDBBackend, dbCreator, false)
}

type MongoDB struct {
	client         *mongo.Client
	databaseName   string
	collectionName string
	collection     *mongo.Collection
	syncCollection *mongo.Collection // For synchronous operations
}

var _ DB = (*MongoDB)(nil)

func NewMongoDB(name string, uri string) (DB, error) {
	return NewMongoDBWithOpts(name, uri, nil)
}

func NewMongoDBWithOpts(name string, uri string, wc *writeconcern.WriteConcern) (DB, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	collection := client.Database("CometBFT-DB").Collection(name)

	if wc == nil {
		// Set to majority write concern if none is provided
		wc = writeconcern.Majority()
	}

	// Create a syncCollection with the provided or default write concern
	syncCollection := client.Database("CometBFT-DB").Collection(name, options.Collection().SetWriteConcern(wc))

	database := &MongoDB{
		client:         client,
		databaseName:   name,
		collectionName: name,
		collection:     collection,
		syncCollection: syncCollection,
	}

	return database, nil
}

func (db *MongoDB) NewBatch() Batch {
	return newMongoDBBatch(db.collection, db.syncCollection)
}

func (db *MongoDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	filter := bson.M{"key": key}
	var result map[string][]byte
	projection := options.FindOne().SetProjection(bson.M{"_id": 0})

	err := db.collection.FindOne(context.Background(), filter, projection).Decode(&result)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return result["value"], nil
}

func (db *MongoDB) Has(key []byte) (bool, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

func (db *MongoDB) Set(key []byte, value []byte) error {
	return db.set(key, value, false)
}

func (db *MongoDB) Delete(key []byte) error {
	return db.delete(key, false)
}

func (db *MongoDB) SetSync(key []byte, value []byte) error {
	return db.set(key, value, true)
}

func (db *MongoDB) DeleteSync(key []byte) error {
	return db.delete(key, true)
}

func (db *MongoDB) set(key []byte, value []byte, sync bool) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	collection := db.collection
	if sync {
		collection = db.syncCollection
	}

	updateOpts := &options.UpdateOptions{}
	updateOpts.SetUpsert(true)
	_, err := collection.UpdateOne(
		context.Background(),
		bson.M{"key": key},
		bson.M{"$set": bson.M{"value": value, "keyString": string(key)}},
		updateOpts,
	)

	return err
}

func (db *MongoDB) delete(key []byte, sync bool) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	collection := db.collection
	if sync {
		collection = db.syncCollection
	}

	_, err := collection.DeleteOne(context.Background(), bson.M{"key": key})
	return err
}

func (db *MongoDB) Close() error {
	return nil // MongoDB driver handles connection pooling
}

func (db *MongoDB) Print() error {
	return nil
	// Implementation here
}

func (db *MongoDB) Stats() map[string]string {
	return nil
	// Implementation here
}
