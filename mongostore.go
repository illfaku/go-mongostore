package mongostore

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Store[V any] struct {
	coll *mongo.Collection
}

type Document[V any] struct {
	Key     string `bson:"_id"`
	Version uint
	Created time.Time
	Updated time.Time
	Value   V
}

type Filter map[string]interface{}

func New[V any](coll *mongo.Collection) *Store[V] {
	if coll == nil {
		panic("collection is nil")
	}
	return &Store[V]{coll}
}

func (store *Store[V]) Find(ctx context.Context, filter Filter) (document *Document[V]) {
	result := store.coll.FindOne(ctx, filter)
	return decodeDocument[V](result)
}

func (store *Store[V]) Get(ctx context.Context, key string) (document *Document[V]) {
	result := store.coll.FindOne(ctx, keyFilter(key))
	return decodeDocument[V](result)
}

func (store *Store[V]) Remove(ctx context.Context, key string) (document *Document[V]) {
	result := store.coll.FindOneAndDelete(ctx, keyFilter(key))
	return decodeDocument[V](result)
}

func (store *Store[V]) Update(
	ctx context.Context,
	key string,
	modify func(value *V) (modified *V, persist bool),
) (document *Document[V]) {
	for i := 0; i < 5; i++ {
		stored := store.Find(ctx, keyFilter(key))
		if stored != nil {
			modified, persist := modify(&stored.Value)
			if persist {
				filter := bson.M{"_id": stored.Key, "version": stored.Version}
				if modified != nil {
					replacement := Document[V]{
						stored.Key,
						stored.Version + 1,
						stored.Created,
						time.Now(),
						*modified,
					}
					result, err := store.coll.ReplaceOne(ctx, filter, replacement)
					if err != nil {
						panic(err)
					}
					if result.MatchedCount != 0 {
						return &replacement
					}
				} else {
					result, err := store.coll.DeleteOne(ctx, filter)
					if err != nil {
						panic(err)
					}
					if result.DeletedCount != 0 {
						return nil
					}
				}
			} else {
				return stored
			}
		} else {
			modified, persist := modify(nil)
			if persist && modified != nil {
				insert := Document[V]{key, 1, time.Now(), time.Now(), *modified}
				update := bson.M{"$setOnInsert": insert}
				result, err := store.coll.UpdateOne(ctx, keyFilter(key), update, options.Update().SetUpsert(true))
				if err != nil {
					panic(err)
				}
				if result.UpsertedCount != 0 {
					return &insert
				}
			} else {
				return nil
			}
		}
	}
	panic("could not update in 5 attempts")
}

func (store *Store[V]) Put(ctx context.Context, key string, value *V) (document *Document[V]) {
	return store.Update(ctx, key, func(*V) (*V, bool) { return value, true })
}

func (store *Store[V]) Create(ctx context.Context, value *V) (document *Document[V]) {
	document = &Document[V]{primitive.NewObjectID().Hex(), 1, time.Now(), time.Now(), *value}
	store.coll.InsertOne(ctx, *document)
	return
}

func keyFilter(key string) Filter {
	return Filter{"_id": key}
}

func decodeDocument[V any](result *mongo.SingleResult) (document *Document[V]) {
	findErr := result.Err()
	if findErr == mongo.ErrNoDocuments {
		return nil
	} else if findErr != nil {
		panic(findErr)
	}

	document = new(Document[V])
	if decodeErr := result.Decode(document); decodeErr != nil {
		panic(decodeErr)
	}
	return
}
