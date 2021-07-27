package limiters

import (
	"context"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
)

// DistLocker is a context aware distributed locker (interface is similar to sync.Locker).
type DistLocker interface {
	// Lock locks the locker.
	Lock(ctx context.Context) error
	// Unlock unlocks the previously successfully locked lock.
	Unlock() error
}

// LockNoop is a no-op implementation of the DistLocker interface.
// It should only be used with the in-memory backends as they are already thread-safe and don't need distributed locks.
type LockNoop struct {
}

// NewLockNoop creates a new LockNoop.
func NewLockNoop() *LockNoop {
	return &LockNoop{}
}

// Lock imitates locking.
func (n LockNoop) Lock(ctx context.Context) error {
	return ctx.Err()
}

// Unlock does nothing.
func (n LockNoop) Unlock() error {
	return nil
}

// LockConsul is a wrapper around github.com/hashicorp/consul/api.Lock that implements the DistLocker interface.
type LockConsul struct {
	lock *api.Lock
}

// NewLockConsul creates a new LockConsul instance.
func NewLockConsul(lock *api.Lock) *LockConsul {
	return &LockConsul{lock: lock}
}

// Lock locks the lock in Consul.
func (l *LockConsul) Lock(ctx context.Context) error {
	_, err := l.lock.Lock(ctx.Done())
	return errors.Wrap(err, "failed to lock a mutex in consul")
}

// Unlock unlocks the lock in Consul.
func (l *LockConsul) Unlock() error {
	return l.lock.Unlock()
}

// LockZookeeper is a wrapper around github.com/samuel/go-zookeeper/zk.Lock that implements the DistLocker interface.
type LockZookeeper struct {
	lock *zk.Lock
}

// NewLockZookeeper creates a new instance of LockZookeeper.
func NewLockZookeeper(lock *zk.Lock) *LockZookeeper {
	return &LockZookeeper{lock: lock}
}

// Lock locks the lock in Zookeeper.
// TODO: add context aware support once https://github.com/samuel/go-zookeeper/pull/168 is merged.
func (l *LockZookeeper) Lock(_ context.Context) error {
	return l.lock.Lock()
}

// Unlock unlocks the lock in Zookeeper.
func (l *LockZookeeper) Unlock() error {
	return l.lock.Unlock()
}
