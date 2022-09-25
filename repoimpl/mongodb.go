package repoimpl

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/zhengchengdong/ARP4G/arp"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongodbStore[T any] struct {
	coll          *mongo.Collection
	newZeroEntity arp.NewZeroEntity[T]
}

func (store *MongodbStore[T]) Load(ctx context.Context, id any) (*T, error) {
	filter := bson.D{{"_id", id}}
	sr := store.coll.FindOne(ctx, filter)
	var err error
	if err = sr.Err(); err == mongo.ErrNoDocuments {
		return nil, nil
	}
	var result bson.D
	sr.Decode(&result)
	var doc []byte
	if doc, err = bson.Marshal(result); err != nil {
		return nil, err
	}
	entity := store.newZeroEntity()
	bson.Unmarshal(doc, entity)
	return entity, nil
}

func (store *MongodbStore[T]) Save(ctx context.Context, id any, entity *T) error {
	_, err := store.coll.InsertOne(ctx, entity)
	return err
}

func (store *MongodbStore[T]) SaveAll(ctx context.Context, entitiesToInsert map[any]any, entitiesToUpdate map[any]*arp.ProcessEntity) error {
	toInsert := make([]any, 0, len(entitiesToInsert))
	for _, v := range entitiesToInsert {
		toInsert = append(toInsert, v)
	}
	if len(toInsert) > 0 {
		_, err := store.coll.InsertMany(ctx, toInsert)
		if err != nil {
			return err
		}
	}
	for k, v := range entitiesToUpdate {
		filter := bson.D{{"_id", k}}
		_, err := store.coll.ReplaceOne(ctx, filter, v.Entity())
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *MongodbStore[T]) RemoveAll(ctx context.Context, ids []any) error {
	for id := range ids {
		filter := bson.D{{"_id", id}}
		_, err := store.coll.DeleteOne(ctx, filter)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewMongodbStore[T any](coll *mongo.Collection, newZeroEntity arp.NewZeroEntity[T]) *MongodbStore[T] {
	return &MongodbStore[T]{coll, newZeroEntity}
}

type MongodbMutexes struct {
	coll           *mongo.Collection
	lockRetryCount int
	maxLockTime    uint64
}

const defaultLockRetryCount = 300
const defaultMaxLockTime = 1 * 60 * 1000

func (mutexes *MongodbMutexes) Lock(ctx context.Context, id any) (ok bool, absent bool, err error) {
	currTime := uint64(time.Now().UnixMilli())
	unlockTime := currTime - mutexes.maxLockTime
	tryOneOk, err := mutexes.tryLock(ctx, id, currTime, unlockTime)
	if err != nil {
		return false, false, err
	}
	if tryOneOk {
		return true, false, nil
	}

	exists, err := mutexes.exists(ctx, id)
	if err != nil {
		return false, false, err
	}
	if !exists {
		return false, true, nil
	}

	retryTimesLeft := mutexes.lockRetryCount
	for retryTimesLeft > 0 {
		tryOneOk, err = mutexes.tryLock(ctx, id, currTime, unlockTime)
		if err != nil {
			return false, false, err
		}
		if tryOneOk {
			return true, false, nil
		}
		retryTimesLeft--
	}
	return false, false, nil
}

func (mutexes *MongodbMutexes) tryLock(ctx context.Context, id any, currTime uint64, unlockTime uint64) (ok bool, err error) {
	filter := bson.D{
		{"$and",
			bson.A{
				bson.D{{"_id", id}},
				bson.D{{"$or", bson.A{
					bson.D{{"state", 0}},
					bson.D{{"time", bson.D{{"$lt", unlockTime}}}},
				}}},
			}},
	}

	update := bson.D{{"$set", bson.D{{"state", 1}, {"time", currTime}}}}
	var updatedDocument bson.M
	err = mutexes.coll.FindOneAndUpdate(ctx, filter, update).Decode(&updatedDocument)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (mutexes *MongodbMutexes) exists(ctx context.Context, id any) (yes bool, err error) {
	filter := bson.D{{"_id", id}}
	var updatedDocument bson.M
	err = mutexes.coll.FindOne(ctx, filter).Decode(&updatedDocument)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (mutexes *MongodbMutexes) NewAndLock(ctx context.Context, id any) (ok bool, err error) {
	currTime := uint64(time.Now().UnixMilli())
	if _, err = mutexes.coll.InsertOne(ctx, bson.D{{"_id", id}, {"state", 1}, {"time", currTime}}); err != nil {
		if mutexes.isDup(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func (mutexes *MongodbMutexes) isDup(err error) bool {
	var e mongo.WriteException
	if errors.As(err, &e) {
		for _, we := range e.WriteErrors {
			if we.Code == 11000 {
				return true
			}
		}
	}
	return false
}

func (mutexes *MongodbMutexes) UnlockAll(ctx context.Context, ids []any) {
	for _, id := range ids {
		filter := bson.D{{"_id", id}}
		update := bson.D{{"$set", bson.D{{"state", 0}}}}
		mutexes.coll.UpdateOne(ctx, filter, update)
	}
}

func NewMongodbMutexes(client *mongo.Client, database string, collection string) *MongodbMutexes {
	return &MongodbMutexes{client.Database(database).Collection("mutexes_" + collection), defaultLockRetryCount, defaultMaxLockTime}
}

func NewMongodbRepository[T any](client *mongo.Client, database string, collection string, newZeroEntity arp.NewZeroEntity[T]) arp.QueryRepository[T] {
	if client == nil {
		return arp.NewMockQueryRepository(newZeroEntity)
	}
	mutexesimpl := NewMongodbMutexes(client, database, collection)
	return NewMongodbRepositoryWithMutexesimpl(client, database, collection, newZeroEntity, mutexesimpl)
}

func NewMongodbRepositoryWithMutexesimpl[T any](client *mongo.Client, database string, collection string, newEmptyEntity arp.NewZeroEntity[T], mutexesimpl arp.Mutexes) arp.QueryRepository[T] {
	if client == nil {
		return arp.NewMockQueryRepository(newEmptyEntity)
	}
	coll := client.Database(database).Collection(collection)
	store := NewMongodbStore(coll, newEmptyEntity)
	return arp.NewQueryRepository[T](arp.NewRepository[T](store, mutexesimpl, newEmptyEntity), NewMongodbQueryFuncs(coll, newEmptyEntity))
}

func NewMongodbQueryFuncs[T any](coll *mongo.Collection, newZeroEntity arp.NewZeroEntity[T]) *MongodbQueryFuncs[T] {
	getEntityId := arp.BuildGetEntityIdFunc[T](reflect.TypeOf(newZeroEntity()).Elem())
	return &MongodbQueryFuncs[T]{coll, newZeroEntity, getEntityId}
}

type MongodbQueryFuncs[T any] struct {
	coll          *mongo.Collection
	newZeroEntity arp.NewZeroEntity[T]
	getEntityId   arp.GetEntityId[T]
}

func (qf *MongodbQueryFuncs[T]) QueryAllIds(ctx context.Context) (ids []any, err error) {
	opts := options.Find().SetProjection(bson.D{})
	cur, err := qf.coll.Find(ctx, bson.D{}, opts)
	if err != nil {
		return nil, err
	}
	var results []bson.D
	if err = cur.All(ctx, &results); err != nil {
		return nil, err
	}

	ids = make([]any, 0)
	for result := range results {
		var doc []byte
		if doc, err = bson.Marshal(result); err != nil {
			return nil, err
		}
		entity := qf.newZeroEntity()
		bson.Unmarshal(doc, entity)
		ids = append(ids, qf.getEntityId(entity))
	}
	return ids, nil
}

func (qf *MongodbQueryFuncs[T]) Count(ctx context.Context) (uint64, error) {
	count, err := qf.coll.EstimatedDocumentCount(ctx)
	return uint64(count), err
}
