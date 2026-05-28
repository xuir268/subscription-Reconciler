package api

import (
	"math/rand"
	"sync"
)

type carrierMock struct {
	mu       sync.Mutex
	statuses []string
	index    int
}

func newCarrierMock() *carrierMock {
	mock := &carrierMock{}
	mock.refillLocked()
	return mock
}

func (m *carrierMock) nextStatus() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.index >= len(m.statuses) {
		m.refillLocked()
	}

	status := m.statuses[m.index]
	m.index++
	return status
}

func (m *carrierMock) refillLocked() {
	m.statuses = m.statuses[:0]
	for i := 0; i < 85; i++ {
		m.statuses = append(m.statuses, "active")
	}
	for i := 0; i < 10; i++ {
		m.statuses = append(m.statuses, "inactive")
	}
	for i := 0; i < 5; i++ {
		m.statuses = append(m.statuses, "api_error")
	}
	rand.Shuffle(len(m.statuses), func(i, j int) {
		m.statuses[i], m.statuses[j] = m.statuses[j], m.statuses[i]
	})
	m.index = 0
}
