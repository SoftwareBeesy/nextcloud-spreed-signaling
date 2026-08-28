//go:build hubloop

/**
 * ISSUE-262 / F141.2 — hub loop watchdog/housekeeping tests (require test hooks on Hub).
 * Enable with: go test -tags hubloop ./server/...
 */
package server

import (
	"bytes"
	"context"
	"encoding/json"
	stdlog "log"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventstest "github.com/strukturag/nextcloud-spreed-signaling/v2/async/events/test"
	signallog "github.com/strukturag/nextcloud-spreed-signaling/v2/log"
	logtest "github.com/strukturag/nextcloud-spreed-signaling/v2/log/test"
)

func TestHub_RoomInCallDropsWhenChannelSaturated(t *testing.T) {
	t.Parallel()
	hub, asyncEvents, _, server := createHubForHubLoopTest(t, false)
	assertHubRoomChannelsBuffered(t, hub)

	msg := testBackendInCallMessage()
	for i := 0; i < cap(hub.roomInCall); i++ {
		hub.roomInCall <- msg
	}
	require.Equal(t, cap(hub.roomInCall), len(hub.roomInCall))

	dropsBefore := hub.roomInCallDropCount()
	room := newHubLoopTestRoom(t, hub, asyncEvents, server)
	runRoomHubNotifyWithoutBlocking(t, room, msg)

	assert.Equal(t, cap(hub.roomInCall), len(hub.roomInCall),
		"full roomInCall channel must not grow beyond capacity")
	assert.Greater(t, hub.roomInCallDropCount(), dropsBefore,
		"roomInCall send must increment drop counter when hub channel is full")
}

func TestHub_WatchdogDetectsStalledEventLoop(t *testing.T) {
	t.Parallel()

	var logBuffer bytes.Buffer
	stdLogger := stdlog.New(&logBuffer, "hub-loop-test: ", stdlog.LstdFlags|stdlog.Lmicroseconds)

	ctx := signallog.NewLoggerContext(t.Context(), stdLogger)
	r := mux.NewRouter()
	registerBackendHandler(t, r)
	server := httptest.NewServer(r)
	t.Cleanup(server.Close)

	asyncEvents := eventstest.GetAsyncEventsForTest(t)
	config, err := getTestConfig(server)
	require.NoError(t, err)

	hub, err := NewHub(ctx, config, asyncEvents, nil, nil, nil, r, "no-version")
	require.NoError(t, err)

	hub.setHubLoopWatchdogIntervalForTest(50 * time.Millisecond)
	go hub.Run()
	t.Cleanup(hub.Stop)

	release := hub.stallHubEventLoopForTest()
	defer release()

	require.Eventually(t, func() bool {
		return hub.hubLoopWatchdogTriggeredForTest() ||
			bytes.Contains(logBuffer.Bytes(), []byte("hub loop stalled"))
	}, 2*time.Second, 20*time.Millisecond, "watchdog must detect stalled Hub.Run and log runtime.Stack")
}

func TestHub_HousekeepingRunsOutsideEventLoop(t *testing.T) {
	t.Parallel()

	hub, _, _, server := createHubForHubLoopTest(t, true)
	hub.setHousekeepingIntervalForTest(100 * time.Millisecond)

	client := NewTestClient(t, server, hub)
	defer client.CloseWithBye()

	release := hub.stallHubEventLoopForTest()
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	message, ok := client.RunUntilMessage(ctx)
	require.True(t, ok, "housekeeping must keep running while Hub.Run event loop is stalled")
	checkMessageType(t, message, "bye")
}

func TestHub_BackendInCallStillAcceptsWhileHubProcessorBacklogged(t *testing.T) {
	t.Parallel()

	hub, _, router, server := createHubForHubLoopTest(t, true)
	config, err := getTestConfig(server)
	require.NoError(t, err)

	ctx := signallog.NewLoggerContext(t.Context(), logtest.NewLoggerForTest(t))
	backend, err := NewBackendServer(ctx, config, hub, "no-version")
	require.NoError(t, err)
	require.NoError(t, backend.Start(router))

	release := hub.stallHubEventLoopForTest()
	defer release()

	roomId := "hub-loop-incall-room"
	var wg sync.WaitGroup
	for i := 0; i < cap(hub.roomInCall)+4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := testBackendInCallMessage()
			data, marshalErr := json.Marshal(msg)
			if marshalErr != nil {
				return
			}
			res, reqErr := performBackendRequest(server.URL+"/api/v1/room/"+roomId, data)
			if reqErr != nil {
				return
			}
			defer res.Body.Close()
			_, _ = io.ReadAll(res.Body)
			assert.Equal(t, http.StatusOK, res.StatusCode)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("backend incall handlers blocked while hub processor was stalled")
	}
}
