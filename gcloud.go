package cachestoregcloudbucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/goware/cachestore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

var (
	ErrUnsupported = errors.New("unsupported")
	ErrNotFound    = errors.New("not found")
)

var _ cachestore.Store[any] = &GCloudStore[any]{}

type GCloudStore[V any] struct {
	options cachestore.StoreOptions
	client  *storage.Client
	bucket  *storage.BucketHandle
}

func New[V any](ctx context.Context, cfg *Config, opts ...cachestore.StoreOptions) (cachestore.Store[V], error) {
	if cfg == nil {
		return nil, errors.New("cfg is nil")
	}

	if !cfg.Enabled {
		return nil, errors.New("attempting to create store while config.Enabled is false")
	}

	if cfg.Bucket == "" {
		return nil, errors.New("bucket name is empty")
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("new GCloud storage client: %w", err)
	}

	store := &GCloudStore[V]{
		options: cachestore.ApplyOptions(opts...),
		client:  client,
		bucket:  client.Bucket(cfg.Bucket),
	}

	return store, nil
}

func (c *GCloudStore[V]) Exists(ctx context.Context, key string) (bool, error) {
	if _, err := c.bucket.Object(key).Attrs(ctx); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}

		return false, fmt.Errorf("get object attrs: %w", err)
	}

	return true, nil
}

func (c *GCloudStore[V]) Set(ctx context.Context, key string, value V) error {
	data, err := serialize(value)
	if err != nil {
		return fmt.Errorf("serialize: %w", err)
	}

	w := c.bucket.Object(key).NewWriter(ctx)

	if _, err = w.Write(data); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	return nil
}

func (c *GCloudStore[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	return ErrUnsupported
}

func (c *GCloudStore[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	if len(keys) == 0 {
		return errors.New("no keys are passed")
	}

	if len(keys) != len(values) {
		return errors.New("keys and values are not the same length")
	}

	g, ctx := errgroup.WithContext(ctx)

	// NOTE: not needed? https://cloud.google.com/storage/quotas
	// g.SetLimit(10)

	for i, key := range keys {
		g.Go(func() error {
			if err := c.Set(ctx, key, values[i]); err != nil {
				return fmt.Errorf("set: %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("errgroup: %w", err)
	}

	return nil
}

func (c *GCloudStore[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	return ErrUnsupported
}

func (c *GCloudStore[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var out V
	obj := c.bucket.Object(key)
	r, err := obj.NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return out, false, storage.ErrObjectNotExist
		}

		return out, false, fmt.Errorf("new reader: %w", err)
	}
	defer r.Close()

	var data []byte
	if _, err := r.Read(data); err != nil {
		return out, false, fmt.Errorf("read: %w", err)
	}

	out, err = deserialize[V](data)
	if err != nil {
		return out, false, err
	}

	return out, true, nil
}

func (c *GCloudStore[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	var mu sync.Mutex
	var errOnce error

	var wg sync.WaitGroup
	wg.Add(len(keys))

	out := make([]V, len(keys))
	oks := make([]bool, len(keys))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, key := range keys {
		go func(i int, key string) {
			defer wg.Done()

			v, ok, err := c.Get(ctx, key)
			if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				cancel()
				mu.Lock()
				defer mu.Unlock()

				if errOnce == nil {
					errOnce = err
				}

				return
			}

			mu.Lock()
			defer mu.Unlock()

			out[i] = v
			oks[i] = ok
		}(i, key)
	}

	wg.Wait()

	return out, oks, errOnce
}

func (c *GCloudStore[V]) Delete(ctx context.Context, key string) error {
	if err := c.bucket.Object(key).Delete(ctx); err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	return nil
}

func (c *GCloudStore[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	iter := c.bucket.Objects(ctx, &storage.Query{Prefix: keyPrefix})

	for {
		attrs, err := iter.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}

			return fmt.Errorf("iterate next: %w", err)
		}

		if err := c.Delete(ctx, attrs.Name); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}

	return nil
}

func (c *GCloudStore[V]) ClearAll(ctx context.Context) error {
	// With gcloud storage, we do not support ClearAll as its too destructive. For testing
	// use the memlru if you want to Clear All.
	return ErrUnsupported
}

func (c *GCloudStore[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	return *new(V), ErrUnsupported
}

func (c *GCloudStore[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	return *new(V), ErrUnsupported
}

func (c *GCloudStore[V]) GCloudClient() *storage.Client {
	return c.client
}

func serialize[V any](value V) ([]byte, error) {
	// return the value directly if the type is a []byte or string,
	// otherwise assume its json and unmarshal it
	switch v := any(value).(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		out, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshall: %w", err)
		}

		return out, nil
	}
}

func deserialize[V any](data []byte) (V, error) {
	var out V

	switch any(out).(type) {
	case string:
		outv := reflect.ValueOf(&out).Elem()
		outv.SetString(string(data))
		return out, nil
	case []byte:
		outv := reflect.ValueOf(&out).Elem()
		outv.SetBytes(data)
		return out, nil
	default:
		if err := json.Unmarshal(data, &out); err != nil {
			return out, fmt.Errorf("unmarshall: %w", err)
		}

		return out, nil
	}
}
