package engine

import (
	"sync"
	"time"

	"lsm/internal/wal"
)

type walSyncer interface {
	AfterAppend(*wal.WAL) error
	OnRotate(*wal.WAL, *wal.WAL) error
	Close() error
	Err() error
}

type policyWalSyncer struct {
	policy   SyncPolicy
	interval time.Duration
	log      func(int, string, ...any)

	mu      sync.Mutex
	active  *wal.WAL
	dirty   bool
	err     error
	stop    chan struct{}
	stopped chan struct{}
}

func newWalSyncer(policy SyncPolicy, interval time.Duration, active *wal.WAL, logger func(int, string, ...any)) walSyncer {
	s := &policyWalSyncer{policy: policy, interval: interval, active: active, log: logger}
	if policy == SyncPolicyEverysec {
		s.stop = make(chan struct{})
		s.stopped = make(chan struct{})
		go s.run()
	}
	return s
}

func (s *policyWalSyncer) AfterAppend(w *wal.WAL) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	switch s.policy {
	case SyncPolicyAlways:
		if err := w.Sync(); err != nil {
			s.setErrorLocked(err)
			return err
		}
	case SyncPolicyEverysec:
		s.active = w
		s.dirty = true
	case SyncPolicyNever:
	}
	return nil
}

func (s *policyWalSyncer) OnRotate(oldWAL, newWAL *wal.WAL) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.policy == SyncPolicyEverysec && s.dirty && oldWAL != nil {
		if err := oldWAL.Sync(); err != nil {
			s.setErrorLocked(err)
			return err
		}
	}
	s.active = newWAL
	s.dirty = false
	return nil
}

func (s *policyWalSyncer) run() {
	ticker := time.NewTicker(s.interval)
	defer func() {
		ticker.Stop()
		close(s.stopped)
	}()
	for {
		select {
		case <-ticker.C:
			s.syncActive()
		case <-s.stop:
			return
		}
	}
}

func (s *policyWalSyncer) syncActive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil || !s.dirty || s.active == nil {
		return
	}
	if err := s.active.Sync(); err != nil {
		s.setErrorLocked(err)
		return
	}
	s.dirty = false
}

func (s *policyWalSyncer) Close() error {
	if s.policy != SyncPolicyEverysec {
		return nil
	}
	close(s.stop)
	<-s.stopped
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.dirty && s.active != nil {
		if err := s.active.Sync(); err != nil {
			s.setErrorLocked(err)
			return err
		}
		s.dirty = false
	}
	return nil
}

func (s *policyWalSyncer) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *policyWalSyncer) setErrorLocked(err error) {
	if s.err == nil {
		s.err = err
		s.log(1, "[WAL] background sync error: %v", err)
	}
}
