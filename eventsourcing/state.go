package eventsourcing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	bucketTimeout = 1 * time.Second
)

func LoadState(nc *nats.Conn, opts *AggregateOptions, key string) (*AggregateState, error) {
	ctx, cancelF := context.WithTimeout(context.Background(), bucketTimeout)
	defer cancelF()

	kv, err := getOrCreateBucket(ctx, nc, opts)
	if err != nil {
		return nil, err
	}

	raw, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return &AggregateState{
				Key:     key,
				Version: 0,
				Data:    nil,
			}, nil
		}
		return nil, err
	}

	var existingState AggregateState
	err = json.Unmarshal(raw.Value(), &existingState)
	if err != nil {
		return nil, err
	}

	return &existingState, nil
}

func StoreState(nc *nats.Conn, opts *AggregateOptions, key string, state AggregateState) error {
	ctx, cancelF := context.WithTimeout(context.Background(), bucketTimeout)
	defer cancelF()

	kv, err := getOrCreateBucket(ctx, nc, opts)
	if err != nil {
		return err
	}

	state.Key = key
	state.Version = state.Version + 1

	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}

	_, err = kv.Put(ctx, key, raw)
	if err != nil {
		return err
	}

	return nil
}

func getOrCreateBucket(ctx context.Context, nc *nats.Conn, opts *AggregateOptions) (jetstream.KeyValue, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	var kv jetstream.KeyValue
	kv, err = js.KeyValue(ctx, opts.StateStoreBucketName)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
				Bucket:      opts.StateStoreBucketName,
				Description: fmt.Sprintf("Persisted state for %s aggregates", opts.AggregateName),
			})
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return kv, nil
}
