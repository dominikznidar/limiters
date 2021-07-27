package limiters

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// TokenBucketState represents a state of a token bucket.
type TokenBucketState struct {
	// Last is the last time the state was updated (Unix timestamp in nanoseconds).
	Last int64
	// Available is the number of available tokens in the bucket.
	Available int64
}

// isZero returns true if the bucket state is zero valued.
func (s TokenBucketState) isZero() bool {
	return s.Last == 0 && s.Available == 0
}

// TokenBucketStateBackend interface encapsulates the logic of retrieving and persisting the state of a TokenBucket.
type TokenBucketStateBackend interface {
	// State gets the current state of the TokenBucket.
	State(ctx context.Context) (TokenBucketState, error)
	// SetState sets (persists) the current state of the TokenBucket.
	SetState(ctx context.Context, state TokenBucketState) error
}

// TokenBucket implements the https://en.wikipedia.org/wiki/Token_bucket algorithm.
type TokenBucket struct {
	locker  DistLocker
	backend TokenBucketStateBackend
	clock   Clock
	logger  Logger
	// refillRate is the tokens refill rate (1 token per duration).
	refillRate time.Duration
	// capacity is the bucket's capacity.
	capacity int64
	mu       sync.Mutex
}

// NewTokenBucket creates a new instance of TokenBucket.
func NewTokenBucket(capacity int64, refillRate time.Duration, locker DistLocker, tokenBucketStateBackend TokenBucketStateBackend, clock Clock, logger Logger) *TokenBucket {
	return &TokenBucket{
		locker:     locker,
		backend:    tokenBucketStateBackend,
		clock:      clock,
		logger:     logger,
		refillRate: refillRate,
		capacity:   capacity,
	}
}

// Take takes tokens from the bucket.
//
// It returns a zero duration and a nil error if the bucket has sufficient amount of tokens.
//
// It returns ErrLimitExhausted if the amount of available tokens is less than requested. In this case the returned
// duration is the amount of time to wait to retry the request.
func (t *TokenBucket) Take(ctx context.Context, tokens int64) (time.Duration, error) {
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
	if state.isZero() {
		// Initially the bucket is full.
		state.Available = t.capacity
	}
	now := t.clock.Now().UnixNano()
	// Refill the bucket.
	tokensToAdd := (now - state.Last) / int64(t.refillRate)
	if tokensToAdd > 0 {
		state.Last = now
		if tokensToAdd+state.Available <= t.capacity {
			state.Available += tokensToAdd
		} else {
			state.Available = t.capacity
		}
	}

	if tokens > state.Available {
		return t.refillRate * time.Duration(tokens-state.Available), ErrLimitExhausted
	}
	// Take the tokens from the bucket.
	state.Available -= tokens
	if err = t.backend.SetState(ctx, state); err != nil {
		return 0, err
	}
	return 0, nil
}

// Limit takes 1 token from the bucket.
func (t *TokenBucket) Limit(ctx context.Context) (time.Duration, error) {
	return t.Take(ctx, 1)
}

// TokenBucketInMemory is an in-memory implementation of TokenBucketStateBackend.
//
// The state is not shared nor persisted so it won't survive restarts or failures.
// Due to the local nature of the state the rate at which some endpoints are accessed can't be reliably predicted or
// limited.
//
// Although it can be used as a global rate limiter with a round-robin load-balancer.
type TokenBucketInMemory struct {
	state TokenBucketState
}

// NewTokenBucketInMemory creates a new instance of TokenBucketInMemory.
func NewTokenBucketInMemory() *TokenBucketInMemory {
	return &TokenBucketInMemory{}
}

// State returns the current bucket's state.
func (t *TokenBucketInMemory) State(ctx context.Context) (TokenBucketState, error) {
	return t.state, ctx.Err()
}

// SetState sets the current bucket's state.
func (t *TokenBucketInMemory) SetState(ctx context.Context, state TokenBucketState) error {
	t.state = state
	return ctx.Err()
}

const (
	redisKeyTBAvailable = "available"
	redisKeyTBLast      = "last"
	redisKeyTBVersion   = "version"
)

func redisKey(prefix, key string) string {
	return fmt.Sprintf("%s/%s", prefix, key)
}

// TokenBucketRedis is a Redis implementation of a TokenBucketStateBackend.
//
// Redis is an in-memory key-value data storage which also supports persistence.
// It is a better choice for high load cases than etcd as it does not keep old values of the keys thus does not need
// the compaction/defragmentation.
//
// Although depending on a persistence and a cluster configuration some data might be lost in case of a failure
// resulting in an under-limiting the accesses to the service.
type TokenBucketRedis struct {
	cli         *redis.Client
	prefix      string
	ttl         time.Duration
	raceCheck   bool
	lastVersion int64
}

// NewTokenBucketRedis creates a new TokenBucketRedis instance.
// Prefix is the key prefix used to store all the keys used in this implementation in Redis.
// TTL is the TTL of the stored keys.
//
// If raceCheck is true and the keys in Redis are modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
// This adds an extra overhead since a Lua script has to be executed on the Redis side which locks the entire database.
func NewTokenBucketRedis(cli *redis.Client, prefix string, ttl time.Duration, raceCheck bool) *TokenBucketRedis {
	return &TokenBucketRedis{cli: cli, prefix: prefix, ttl: ttl, raceCheck: raceCheck}
}

// State gets the bucket's state from Redis.
func (t *TokenBucketRedis) State(ctx context.Context) (TokenBucketState, error) {
	var values []interface{}
	var err error
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		keys := []string{
			redisKey(t.prefix, redisKeyTBLast),
			redisKey(t.prefix, redisKeyTBAvailable),
		}
		if t.raceCheck {
			keys = append(keys, redisKey(t.prefix, redisKeyTBVersion))
		}
		values, err = t.cli.MGet(ctx, keys...).Result()
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return TokenBucketState{}, ctx.Err()
	}

	if err != nil {
		return TokenBucketState{}, errors.Wrap(err, "failed to get keys from redis")
	}
	nilAny := false
	for _, v := range values {
		if v == nil {
			nilAny = true
			break
		}
	}
	if nilAny || err == redis.Nil {
		// Keys don't exist, return the initial state.
		return TokenBucketState{}, nil
	}

	last, err := strconv.ParseInt(values[0].(string), 10, 64)
	if err != nil {
		return TokenBucketState{}, err
	}
	available, err := strconv.ParseInt(values[1].(string), 10, 64)
	if err != nil {
		return TokenBucketState{}, err
	}
	if t.raceCheck {
		t.lastVersion, err = strconv.ParseInt(values[2].(string), 10, 64)
		if err != nil {
			return TokenBucketState{}, err
		}
	}
	return TokenBucketState{
		Last:      last,
		Available: available,
	}, nil
}

// SetState updates the state in Redis.
func (t *TokenBucketRedis) SetState(ctx context.Context, state TokenBucketState) error {
	var err error
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		if !t.raceCheck {
			_, err = t.cli.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
				if err = pipeliner.Set(ctx, redisKey(t.prefix, redisKeyTBLast), state.Last, t.ttl).Err(); err != nil {
					return err
				}
				return pipeliner.Set(ctx, redisKey(t.prefix, redisKeyTBAvailable), state.Available, t.ttl).Err()
			})
			return
		}
		var result interface{}
		// TODO: use EVALSHA.
		result, err = t.cli.Eval(ctx, `
	local version = tonumber(redis.call('get', KEYS[1])) or 0
	if version > tonumber(ARGV[1]) then
		return 'RACE_CONDITION'
	end
	return {
		redis.call('incr', KEYS[1]),
		redis.call('pexpire', KEYS[1], ARGV[4]),
		redis.call('set', KEYS[2], ARGV[2], 'PX', ARGV[4]),
		redis.call('set', KEYS[3], ARGV[3], 'PX', ARGV[4]),
	}
	`, []string{
			redisKey(t.prefix, redisKeyTBVersion),
			redisKey(t.prefix, redisKeyTBLast),
			redisKey(t.prefix, redisKeyTBAvailable),
		},
			t.lastVersion,
			state.Last,
			state.Available,
			// TTL in milliseconds.
			int64(t.ttl/time.Microsecond)).Result()

		if err == nil {
			err = checkResponseFromRedis(result, []interface{}{t.lastVersion + 1, int64(1), "OK", "OK"})
		}
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return ctx.Err()
	}

	return errors.Wrap(err, "failed to save keys to redis")
}
