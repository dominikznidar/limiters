package limiters

import (
	"context"
	"reflect"
	"strconv"
	"sync"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// LeakyBucketState represents the state of a LeakyBucket.
type LeakyBucketState struct {
	// Last is the Unix timestamp in nanoseconds of the most recent request.
	Last int64
}

// IzZero returns true if the bucket state is zero valued.
func (s LeakyBucketState) IzZero() bool {
	return s.Last == 0
}

// LeakyBucketStateBackend interface encapsulates the logic of retrieving and persisting the state of a LeakyBucket.
type LeakyBucketStateBackend interface {
	// State gets the current state of the LeakyBucket.
	State(ctx context.Context) (LeakyBucketState, error)
	// SetState sets (persists) the current state of the LeakyBucket.
	SetState(ctx context.Context, state LeakyBucketState) error
}

// LeakyBucket implements the https://en.wikipedia.org/wiki/Leaky_bucket#As_a_queue algorithm.
type LeakyBucket struct {
	locker  DistLocker
	backend LeakyBucketStateBackend
	clock   Clock
	logger  Logger
	// Capacity is the maximum allowed number of tockens in the bucket.
	capacity int64
	// Rate is the output rate: 1 request per the rate duration (in nanoseconds).
	rate int64
	mu   sync.Mutex
}

// NewLeakyBucket creates a new instance of LeakyBucket.
func NewLeakyBucket(capacity int64, rate time.Duration, locker DistLocker, leakyBucketStateBackend LeakyBucketStateBackend, clock Clock, logger Logger) *LeakyBucket {
	return &LeakyBucket{
		locker:   locker,
		backend:  leakyBucketStateBackend,
		clock:    clock,
		logger:   logger,
		capacity: capacity,
		rate:     int64(rate),
	}
}

// Limit returns the time duration to wait before the request can be processed.
// If the last request happened earlier than the rate this method returns zero duration.
// It returns ErrLimitExhausted if the the request overflows the bucket's capacity. In this case the returned duration
// means how long it would have taken to wait for the request to be processed if the bucket was not overflowed.
func (t *LeakyBucket) Limit(ctx context.Context) (time.Duration, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.locker.Lock(ctx); err != nil {
		return 0, err
	}
	defer func() {
		if err := t.locker.Unlock(); err != nil {
			t.logger.Log(err)
		}
	}()
	state, err := t.backend.State(ctx)
	if err != nil {
		return 0, err
	}
	now := t.clock.Now().UnixNano()
	if now < state.Last {
		// The queue has requests in it: move the current request to the last position + 1.
		state.Last += t.rate
	} else {
		// The queue is empty.
		// The offset is the duration to wait in case the last request happened less than rate duration ago.
		var offset int64
		delta := now - state.Last
		if delta < t.rate {
			offset = t.rate - delta
		}
		state.Last = now + offset
	}

	wait := state.Last - now
	if wait/t.rate > t.capacity {
		return time.Duration(wait), ErrLimitExhausted
	}
	if err = t.backend.SetState(ctx, state); err != nil {
		return 0, err
	}
	return time.Duration(wait), nil
}

// LeakyBucketInMemory is an in-memory implementation of LeakyBucketStateBackend.
type LeakyBucketInMemory struct {
	state LeakyBucketState
}

// NewLeakyBucketInMemory creates a new instance of LeakyBucketInMemory.
func NewLeakyBucketInMemory() *LeakyBucketInMemory {
	return &LeakyBucketInMemory{}
}

// State gets the current state of the bucket.
func (l *LeakyBucketInMemory) State(ctx context.Context) (LeakyBucketState, error) {
	return l.state, ctx.Err()
}

// SetState sets the current state of the bucket.
func (l *LeakyBucketInMemory) SetState(ctx context.Context, state LeakyBucketState) error {
	l.state = state
	return ctx.Err()
}

const (
	redisKeyLBLast    = "last"
	redisKeyLBVersion = "version"
)

// LeakyBucketRedis is a Redis implementation of a LeakyBucketStateBackend.
type LeakyBucketRedis struct {
	cli         *redis.Client
	prefix      string
	ttl         time.Duration
	raceCheck   bool
	lastVersion int64
}

// NewLeakyBucketRedis creates a new LeakyBucketRedis instance.
// Prefix is the key prefix used to store all the keys used in this implementation in Redis.
// TTL is the TTL of the stored keys.
//
// If raceCheck is true and the keys in Redis are modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
func NewLeakyBucketRedis(cli *redis.Client, prefix string, ttl time.Duration, raceCheck bool) *LeakyBucketRedis {
	return &LeakyBucketRedis{cli: cli, prefix: prefix, ttl: ttl, raceCheck: raceCheck}
}

// State gets the bucket's state from Redis.
func (t *LeakyBucketRedis) State(ctx context.Context) (LeakyBucketState, error) {
	var values []interface{}
	var err error
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		keys := []string{
			redisKey(t.prefix, redisKeyLBLast),
		}
		if t.raceCheck {
			keys = append(keys, redisKey(t.prefix, redisKeyLBVersion))
		}
		values, err = t.cli.MGet(ctx, keys...).Result()
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return LeakyBucketState{}, ctx.Err()
	}

	if err != nil {
		return LeakyBucketState{}, errors.Wrap(err, "failed to get keys from redis")
	}
	nilAny := false
	for _, v := range values {
		if v == nil {
			nilAny = true
			break
		}
	}
	if nilAny || err == redis.Nil {
		// Keys don't exist, return an empty state.
		return LeakyBucketState{}, nil
	}

	last, err := strconv.ParseInt(values[0].(string), 10, 64)
	if err != nil {
		return LeakyBucketState{}, err
	}
	if t.raceCheck {
		t.lastVersion, err = strconv.ParseInt(values[1].(string), 10, 64)
		if err != nil {
			return LeakyBucketState{}, err
		}
	}
	return LeakyBucketState{
		Last: last,
	}, nil
}

func checkResponseFromRedis(response interface{}, expected interface{}) error {
	if s, sok := response.(string); sok && s == "RACE_CONDITION" {
		return ErrRaceCondition
	}
	if !reflect.DeepEqual(response, expected) {
		return errors.Errorf("got %+v from redis, expected %+v", response, expected)
	}
	return nil
}

// SetState updates the state in Redis.
// The provided fencing token is checked on the Redis side before saving the keys.
func (t *LeakyBucketRedis) SetState(ctx context.Context, state LeakyBucketState) error {
	var err error
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		if !t.raceCheck {
			err = t.cli.Set(ctx, redisKey(t.prefix, redisKeyLBLast), state.Last, t.ttl).Err()
			return
		}
		var result interface{}
		// TODO: make use of EVALSHA.
		result, err = t.cli.Eval(ctx, `
	local version = tonumber(redis.call('get', KEYS[1])) or 0
	if version > tonumber(ARGV[1]) then
		return 'RACE_CONDITION'
	end
	return {
		redis.call('incr', KEYS[1]),
		redis.call('pexpire', KEYS[1], ARGV[3]),
		redis.call('set', KEYS[2], ARGV[2], 'PX', ARGV[3]),
	}
	`, []string{
			redisKey(t.prefix, redisKeyLBVersion),
			redisKey(t.prefix, redisKeyLBLast),
		},
			t.lastVersion,
			state.Last,
			// TTL in milliseconds.
			int64(t.ttl/time.Microsecond)).Result()

		if err == nil {
			err = checkResponseFromRedis(result, []interface{}{t.lastVersion + 1, int64(1), "OK"})
		}
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return ctx.Err()
	}

	return errors.Wrap(err, "failed to save keys to redis")
}
