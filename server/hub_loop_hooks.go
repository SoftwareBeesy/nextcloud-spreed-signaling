//go:build hubloop

package server

import "time"

func (h *Hub) roomInCallDropCount() uint64 {
	return h.roomInCallDrops.Load()
}

func (h *Hub) stallHubEventLoopForTest() func() {
	h.hubLoopStallMu.Lock()
	h.hubLoopStallRelease = make(chan struct{})
	h.hubLoopStallMu.Unlock()

	return func() {
		h.hubLoopStallMu.Lock()
		if ch := h.hubLoopStallRelease; ch != nil {
			close(ch)
			h.hubLoopStallRelease = nil
		}
		h.hubLoopStallMu.Unlock()
	}
}

func (h *Hub) setHubLoopWatchdogIntervalForTest(interval time.Duration) {
	h.hubLoopWatchdogIntervalNanos.Store(int64(interval))
}

func (h *Hub) hubLoopWatchdogTriggeredForTest() bool {
	return h.hubLoopWatchdogTriggered.Load()
}

func (h *Hub) setHousekeepingIntervalForTest(interval time.Duration) {
	h.housekeepingIntervalNanos.Store(int64(interval))
}
