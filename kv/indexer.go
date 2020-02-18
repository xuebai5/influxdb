package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	influxdb "github.com/influxdata/influxdb"
	"go.uber.org/zap"
)

// IndexSource is a type which can be used as the source for
// populating, walking and managing an index.
type IndexSource interface {
	Bucket(tx Tx) (Bucket, error)

	IndexOn(body []byte) (key influxdb.ID, err error)
}

type source struct {
	bucket []byte

	indexOn func(b []byte) (key influxdb.ID, err error)
}

func (s *source) Bucket(tx Tx) (Bucket, error) {
	return tx.Bucket(s.bucket)
}

func (s *source) IndexOn(v []byte) (influxdb.ID, error) {
	return s.indexOn(v)
}

// NewSource takes a bucket name and an index on function and returns
// an IndexSource for use in an Indexer
func NewSource(bucket []byte, fn func([]byte) (influxdb.ID, error)) IndexSource {
	return &source{bucket, fn}
}

// Indexer is used to define and manage an index for a source bucket.
//
// When using the indexer you must provide it with an IndexSource.
// The IndexSource provides the indexer with the contract it needs to populate
// the entire index and traverse a populated index correctly.
// The IndexSource provides a way to retrieve the key on which to index with
// when provided with the value from the source.
// It also provides the way to access the source bucket.
//
// The following is an illustration of its use:
//
//  byUserID := func(v []byte) (influxdb.ID, error) {
//      auth := &influxdb.Authorization{}
//
//      if err := json.Unmarshal(v, auth); err != nil {
//          return err
//      }
//
//      return auth.UserID, nil
//  }
//
//  // configure a write only indexer
//  indexByUser := NewIndexer(NewSource([]byte(`authorizationsbyuserv1/), byUserID), false)
//
//  indexByUser.Insert(tx, someUserID, someAuthID)
//
//  indexByUser.Delete(tx, someUserID, someAuthID)
//
//  indexByUser.Walk(tx, someUserID, func(k, v []byte) error {
//      auth := &influxdb.Authorization{}
//      if err := json.Unmarshal(v, auth); err != nil {
//          return err
//      }
//
//      // do something with auth
//
//      return nil
//  })
//
//  // populate entire index from source
//  indexedCount, err := indexByUser.Populate(ctx, store)
type Indexer struct {
	indexBkt []byte
	source   IndexSource

	log *zap.Logger

	// populateBatchSize configures the size of the batch used for insertion
	populateBatchSize int
	// canRead configures whether or not Walk accesses the index at all
	// or skips the index altogether and returns nothing.
	// This is used when you want to integrate only the write path before
	// releasing the read path.
	canRead bool
}

// NewIndexer configures and returns a new Indexer isntance.
func NewIndexer(src IndexSource, indexBucket []byte, canRead bool) *Indexer {
	indexer := &Indexer{
		indexBkt:          indexBucket,
		source:            src,
		populateBatchSize: 100,
		log:               zap.NewNop(),
		canRead:           canRead,
	}

	return indexer
}

func (i *Indexer) indexBucket(tx Tx) (Bucket, error) {
	return tx.Bucket(i.indexBkt)
}

func (i *Indexer) sourceBucket(tx Tx) (Bucket, error) {
	return i.source.Bucket(tx)
}

func indexKey(foreignKey, primaryKey influxdb.ID) (newKey, pkID []byte, _ error) {
	fkID, err := foreignKey.Encode()
	if err != nil {
		return nil, nil, err
	}

	pkID, err = primaryKey.Encode()
	if err != nil {
		return nil, nil, err
	}

	newKey = make([]byte, len(pkID)+len(fkID)+1)
	copy(newKey, fkID)
	newKey[len(fkID)] = '/'
	copy(newKey[len(fkID)+1:], pkID)

	return
}

func indexKeyParts(indexKey []byte) (fk, pk influxdb.ID, err error) {
	// this function is called with items missing in index
	parts := bytes.SplitN(indexKey, []byte("/"), 2)
	if len(parts) < 2 {
		return pk, fk, errors.New("malformed index key")
	}

	// parts are fk/pk
	if err = fk.Decode(parts[0]); err != nil {
		return
	}

	if err = pk.Decode(parts[1]); err != nil {
		return
	}

	return
}

// Insert creates a single index entry for the provided primary key on the foreign key.
func (i *Indexer) Insert(tx Tx, foreignKey, primaryKey influxdb.ID) error {
	newKey, pkID, err := indexKey(foreignKey, primaryKey)
	if err != nil {
		return err
	}

	bkt, err := i.indexBucket(tx)
	if err != nil {
		return err
	}

	return bkt.Put(newKey, pkID)
}

// Delete removes the foreignKey and primaryKey mapping from the underlying index.
func (i *Indexer) Delete(tx Tx, foreignKey, primaryKey influxdb.ID) error {
	newKey, _, err := indexKey(foreignKey, primaryKey)
	if err != nil {
		return err
	}

	bkt, err := i.indexBucket(tx)
	if err != nil {
		return err
	}

	return bkt.Delete(newKey)
}

// VisitFunc is called for each k, v byte slice pair from the underlying source bucket
// which are found in the index bucket for a provided foreign key.
type VisitFunc func(k, v []byte) error

// Walk walks the source bucket using keys found in the index using the provided foreign key
// given the index has been fully populated.
func (i *Indexer) Walk(tx Tx, foreignKey []byte, visitFn VisitFunc) error {
	// skip walking if configured to do so as the indexer
	// is currently being used purely to write index
	if !i.canRead {
		return nil
	}

	sourceBucket, err := i.source.Bucket(tx)
	if err != nil {
		return err
	}

	indexBucket, err := i.indexBucket(tx)
	if err != nil {
		return err
	}

	cursor, err := indexBucket.ForwardCursor(foreignKey,
		WithCursorPrefix(foreignKey))
	if err != nil {
		return err
	}

	return indexWalk(cursor, sourceBucket, visitFn, func(fk, pk []byte) error {
		// fail iteration when key not found for item in index
		return fmt.Errorf("for key %v indexed by %v: %w", pk, fk, ErrKeyNotFound)
	})
}

// Populate does a full population of the index using the provided IndexOnFunc.
// Once completed it marks the index as ready for use.
// It return a nil error on success and the count of inserted items.
func (i *Indexer) Populate(ctx context.Context, store Store) (n int, err error) {
	var (
		seek []byte
		more = true
	)

	for more {
		if err = store.Update(ctx, func(tx Tx) error {
			// initially set more to false
			more = false

			sourceBucket, err := i.source.Bucket(tx)
			if err != nil {
				return err
			}

			indexBucket, err := i.indexBucket(tx)
			if err != nil {
				return err
			}

			cursor, err := sourceBucket.ForwardCursor(seek,
				WithCursorSkipFirstItem(),
				WithCursorLimit(i.populateBatchSize))
			if err != nil {
				return err
			}

			return i.missingIndexWalk(cursor, indexBucket, func(indexKey, pk []byte) error {
				// if we find at-least one key then we assume there are still more
				more = true

				// insert missing item into index
				if err := indexBucket.Put(indexKey, pk); err != nil {
					return err
				}

				// increment added to index func
				n++
				// set next seek location to last found primary key
				seek = pk

				return nil
			})
		}); err != nil {
			return
		}
	}

	return
}

// IndexDiff contains a set of items present in the source not in index,
// along with a set of things in the index which are not in the source.
type IndexDiff struct {
	Source map[influxdb.ID]influxdb.ID
	Index  map[influxdb.ID]influxdb.ID
}

func (i *IndexDiff) addMissingSource(pk, fk influxdb.ID) {
	if i.Source == nil {
		i.Source = map[influxdb.ID]influxdb.ID{}
	}

	i.Source[pk] = fk
}

func (i *IndexDiff) addMissingIndex(pk, fk influxdb.ID) {
	if i.Index == nil {
		i.Index = map[influxdb.ID]influxdb.ID{}
	}

	i.Index[pk] = fk
}

// Verify returns returns difference between a source and its index
// The difference contains items in the source that are not in the index
// and vice-versa.
func (i *Indexer) Verify(ctx context.Context, tx Tx) (diff IndexDiff, err error) {
	sourceBucket, err := i.source.Bucket(tx)
	if err != nil {
		return
	}

	indexBucket, err := i.indexBucket(tx)
	if err != nil {
		return
	}

	// create cursor for entire index
	cursor, err := indexBucket.ForwardCursor(nil)
	if err != nil {
		return
	}

	if err = indexWalk(cursor, sourceBucket, func(k, v []byte) error {
		// we're only interested in index items not in source
		return nil
	}, func(k, v []byte) error {
		fk, pk, err := indexKeyParts(k)
		if err != nil {
			return err
		}

		diff.addMissingSource(pk, fk)

		// continue iterating over index
		return nil
	}); err != nil {
		return
	}

	// create a new cursor over the source and look for items
	// missing from the index
	cursor, err = sourceBucket.ForwardCursor(nil)
	if err != nil {
		return
	}

	if err = i.missingIndexWalk(cursor, indexBucket, func(indexKey, pk []byte) error {
		fkID, pkID, err := indexKeyParts(indexKey)
		if err != nil {
			return err
		}

		// add missing item in source not in index
		diff.addMissingIndex(pkID, fkID)

		return nil
	}); err != nil {
		return
	}

	return
}

type notFoundFunc func(fk, pk []byte) error

func indexWalk(cursor ForwardCursor, sourceBucket Bucket, visit VisitFunc, notFound notFoundFunc) error {
	for k, v := cursor.Next(); k != nil; k, v = cursor.Next() {
		// lookup index value as source key
		sv, err := sourceBucket.Get(v)
		if err != nil {
			if IsNotFound(err) {
				// delegate to not found function to decide
				// whether or not to keep iterating
				if err := notFound(k, v); err != nil {
					return err
				}

				// give nil error from not found continue searching
				continue
			}

			return err
		}

		if err := visit(v, sv); err != nil {
			return err
		}
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	return cursor.Close()
}

func (i *Indexer) missingIndexWalk(cursor ForwardCursor, indexBucket Bucket, missing func(indexKey, pk []byte) error) error {
	return consumeCursor(cursor, func(pkID influxdb.ID, v []byte) error {
		fkID, err := i.source.IndexOn(v)
		if err != nil {
			return err
		}

		indexKey, pk, err := indexKey(fkID, pkID)
		if err != nil {
			return err
		}

		if _, err := indexBucket.Get(indexKey); err != nil {
			if IsNotFound(err) {
				if err := missing(indexKey, pk); err != nil {
					return err
				}

				return nil
			}

			return err
		}

		return nil
	})
}

func consumeCursor(cursor ForwardCursor, fn func(pk influxdb.ID, v []byte) error) error {
	for k, v := cursor.Next(); k != nil; k, v = cursor.Next() {
		var pkID influxdb.ID
		if err := pkID.Decode(k); err != nil {
			return err
		}

		if err := fn(pkID, v); err != nil {
			return err
		}
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	return cursor.Close()
}
