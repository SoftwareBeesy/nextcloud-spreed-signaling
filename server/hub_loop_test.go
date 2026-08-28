/**
 * ISSUE-262 / F141.2 — hub loop structural fix (RED phase).
 * Tests describe expected behavior from RCA; they fail until implementer lands the surgical patch.
 */
package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/strukturag/nextcloud-spreed-signaling/v2/api"
	"github.com/strukturag/nextcloud-spreed-signaling/v2/async/events"
	eventstest "github.com/strukturag/nextcloud-spreed-signaling/v2/async/events/test"
	signallog "github.com/strukturag/nextcloud-spreed-signaling/v2/log"
	logtest "github.com/strukturag/nextcloud-spreed-signaling/v2/log/test"
	"github.com/strukturag/nextcloud-spreed-signaling/v2/talk"
)

const (
	hubLoopBackpressureTimeout = 500 * time.Millisecond
	hubLoopMinChannelBuffer    = 16
)

func createHubForHubLoopTest(t *testing.T, startRun bool) (*Hub, events.AsyncEvents, *mux.Router, *httptest.Server) {
	t.Helper()

	logger := logtest.NewLoggerForTest(t)
	ctx := signallog.NewLoggerContext(t.Context(), logger)
	require := require.New(t)

	r := mux.NewRouter()
	registerBackendHandler(t, r)

	server := httptest.NewServer(r)
	t.Cleanup(server.Close)

	asyncEvents := eventstest.GetAsyncEventsForTest(t)
	config, err := getTestConfig(server)
	require.NoError(err)

	hub, err := NewHub(ctx, config, asyncEvents, nil, nil, nil, r, "no-version")
	require.NoError(err)

	backend, err := NewBackendServer(ctx, config, hub, "no-version")
	require.NoError(err)
	require.NoError(backend.Start(r))

	if startRun {
		go hub.Run()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()
			WaitForHub(ctx, t, hub)
		})
	} else {
		t.Cleanup(func() {
			hub.Stop()
		})
	}

	return hub, asyncEvents, r, server
}

func newHubLoopTestRoom(t *testing.T, hub *Hub, asyncEvents events.AsyncEvents, server *httptest.Server) *Room {
	t.Helper()

	u, err := url.Parse(server.URL)
	require.NoError(t, err)

	backend := hub.backend.GetBackend(u)
	require.NotNil(t, backend)

	room, err := NewRoom("hub-loop-test-room", nil, hub, asyncEvents, backend)
	require.NoError(t, err)
	return room
}

func testBackendInCallMessage() *talk.BackendServerRoomRequest {
	return &talk.BackendServerRoomRequest{
		Type: "incall",
		InCall: &talk.BackendRoomInCallRequest{
			InCall: json.RawMessage(strconv.FormatInt(FlagInCall, 10)),
		},
	}
}

func testBackendRoomUpdateMessage() *talk.BackendServerRoomRequest {
	return &talk.BackendServerRoomRequest{
		Type: "update",
		Update: &talk.BackendRoomUpdateRequest{
			Properties: testRoomProperties,
		},
	}
}

func testBackendRoomDeleteMessage() *talk.BackendServerRoomRequest {
	return &talk.BackendServerRoomRequest{
		Type: "delete",
	}
}

func testBackendRoomParticipantsMessage() *talk.BackendServerRoomRequest {
	return &talk.BackendServerRoomRequest{
		Type: "participants",
		Participants: &talk.BackendRoomParticipantsRequest{
			Changed: []api.StringMap{
				{"inCall": json.RawMessage("0")},
			},
		},
	}
}

func assertHubRoomChannelsBuffered(t *testing.T, hub *Hub) {
	t.Helper()
	assert.Greater(t, cap(hub.roomUpdated), 0, "roomUpdated must be buffered")
	assert.Greater(t, cap(hub.roomDeleted), 0, "roomDeleted must be buffered")
	assert.Greater(t, cap(hub.roomInCall), 0, "roomInCall must be buffered")
	assert.Greater(t, cap(hub.roomParticipants), 0, "roomParticipants must be buffered")
	assert.GreaterOrEqual(t, cap(hub.roomInCall), hubLoopMinChannelBuffer,
		"roomInCall buffer should absorb incall bursts without global deadlock")
}

func runRoomHubNotifyWithoutBlocking(t *testing.T, room *Room, message *talk.BackendServerRoomRequest) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		room.processBackendRoomRequestRoom(message)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(hubLoopBackpressureTimeout):
		t.Fatal("room goroutine blocked sending to hub channel under backpressure")
	}
}

func TestHub_RoomEventChannelsAreBuffered(t *testing.T) {
	t.Parallel()
	hub, _, _, _ := createHubForHubLoopTest(t, true)
	assertHubRoomChannelsBuffered(t, hub)
}

func TestRoom_HubNotifyDoesNotBlockUnderBackpressure(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		message *talk.BackendServerRoomRequest
	}{
		{name: "roomInCall", message: testBackendInCallMessage()},
		{name: "roomUpdated", message: testBackendRoomUpdateMessage()},
		{name: "roomDeleted", message: testBackendRoomDeleteMessage()},
		{name: "roomParticipants", message: testBackendRoomParticipantsMessage()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hub, asyncEvents, _, server := createHubForHubLoopTest(t, false)
			room := newHubLoopTestRoom(t, hub, asyncEvents, server)
			runRoomHubNotifyWithoutBlocking(t, room, tc.message)
		})
	}
}
