package cip

import (
	"testing"
	"time"
)

func TestKeyedLocksSerializesSameKey(t *testing.T) {
	kl := newKeyedLocks()

	unlockFirst := kl.lock("same-entity")

	acquired := make(chan func())

	go func() {
		unlockSecond := kl.lock("same-entity")
		acquired <- unlockSecond
	}()

	select {
	case unlockSecond := <-acquired:
		unlockSecond()
		t.Fatal("second lock acquisition should block while the first lock is held")
	case <-time.After(25 * time.Millisecond):
	}

	unlockFirst()

	select {
	case unlockSecond := <-acquired:
		unlockSecond()
	case <-time.After(250 * time.Millisecond):
		t.Fatal("second lock acquisition did not proceed after the first lock was released")
	}

	if len(kl.locks) != 0 {
		t.Fatalf("expected lock registry to be empty after releasing both locks, got %d entries", len(kl.locks))
	}
}

func TestKeyedLocksAllowsDifferentKeysInParallel(t *testing.T) {
	kl := newKeyedLocks()

	unlockFirst := kl.lock("entity-a")

	acquired := make(chan func())

	go func() {
		unlockSecond := kl.lock("entity-b")
		acquired <- unlockSecond
	}()

	select {
	case unlockSecond := <-acquired:
		unlockSecond()
	case <-time.After(250 * time.Millisecond):
		t.Fatal("lock acquisition for a different key should not block")
	}

	unlockFirst()

	if len(kl.locks) != 0 {
		t.Fatalf("expected lock registry to be empty after releasing both locks, got %d entries", len(kl.locks))
	}
}

func TestKeyedLocksRemovesEntryAfterLastUnlock(t *testing.T) {
	kl := newKeyedLocks()

	unlock := kl.lock("entity-a")

	entry, ok := kl.locks["entity-a"]
	if !ok {
		t.Fatal("expected lock entry to exist while locked")
	}

	if entry.refCount != 1 {
		t.Fatalf("expected refCount to be 1 while locked, got %d", entry.refCount)
	}

	unlock()

	if len(kl.locks) != 0 {
		t.Fatalf("expected lock entry to be removed after unlock, got %d entries", len(kl.locks))
	}
}
