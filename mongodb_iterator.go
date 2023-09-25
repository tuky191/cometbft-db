package db

import (
	"bytes"
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBIterator struct {
	cursor    *mongo.Cursor
	start     []byte
	end       []byte
	isReverse bool
	isInvalid bool
	lastErr   error
	current   map[string][]byte
}

func newMongoDBIterator(cursor *mongo.Cursor, start, end []byte, isReverse bool) *MongoDBIterator {
	return &MongoDBIterator{
		cursor:    cursor,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

func (itr *MongoDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *MongoDBIterator) Valid() bool {
	if itr.isInvalid {
		return false
	}

	if err := itr.Error(); err != nil {
		itr.isInvalid = true
		return false
	}

	err := itr.cursor.Decode(&itr.current)
	if err != nil {
		itr.lastErr = err
		itr.isInvalid = true
		return false
	}

	key := itr.current["key"]

	if itr.isReverse {
		if itr.start != nil && bytes.Compare(key, itr.start) < 0 {
			itr.isInvalid = true
			return false
		}
	} else {
		if itr.end != nil && bytes.Compare(itr.end, key) <= 0 {
			itr.isInvalid = true
			return false
		}
	}

	return true
}

func (itr *MongoDBIterator) Key() []byte {
	itr.assertIsValid()
	return itr.current["key"]
}

func (itr *MongoDBIterator) Value() []byte {
	itr.assertIsValid()
	return itr.current["value"]
}

func (itr *MongoDBIterator) Next() {
	itr.assertIsValid()

	if !itr.cursor.Next(context.Background()) {
		itr.isInvalid = true
		return
	}
	err := itr.cursor.Decode(&itr.current)
	if err != nil {
		log.Panic("unable to decode current cursor")
	}
}

func (itr *MongoDBIterator) Error() error {
	return itr.lastErr
}

func (itr *MongoDBIterator) Close() error {
	return itr.cursor.Close(context.Background())
}

func (itr *MongoDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}

func (db *MongoDB) createIterator(start, end []byte, sortDirection int) (Iterator, error) {
	var filter primitive.M

	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}

	switch {
	case start == nil && end == nil:
		filter = bson.M{}
	case start == nil:
		filter = bson.M{
			"key": bson.M{
				"$lt": end,
			},
		}
	case end == nil:
		filter = bson.M{
			"key": bson.M{
				"$gte": start,
			},
		}
	default:
		filter = bson.M{
			"key": bson.M{
				"$gte": start,
				"$lt":  end,
			},
		}
	}

	opts := options.Find().SetSort(bson.M{"key": sortDirection}).SetProjection(bson.M{"_id": 0})

	cursor, err := db.collection.Find(context.Background(), filter, opts)
	if err != nil {
		return nil, err
	}

	cursor.Next(context.Background())
	isReverse := sortDirection == -1
	return newMongoDBIterator(cursor, start, end, isReverse), nil
}

func (db *MongoDB) Iterator(start, end []byte) (Iterator, error) {
	return db.createIterator(start, end, 1)
}

func (db *MongoDB) ReverseIterator(start, end []byte) (Iterator, error) {
	return db.createIterator(start, end, -1)
}
