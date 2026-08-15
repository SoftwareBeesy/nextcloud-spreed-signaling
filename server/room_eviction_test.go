package server

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/strukturag/nextcloud-spreed-signaling/v2/api"
	"github.com/strukturag/nextcloud-spreed-signaling/v2/session"
	"github.com/strukturag/nextcloud-spreed-signaling/v2/talk"
)

type evictionTestSession struct {
	publicId   api.PublicSessionId
	userId     string
	clientType api.ClientType
}

func (s *evictionTestSession) Context() context.Context              { return context.Background() }
func (s *evictionTestSession) PrivateId() api.PrivateSessionId       { return "" }
func (s *evictionTestSession) PublicId() api.PublicSessionId         { return s.publicId }
func (s *evictionTestSession) ClientType() api.ClientType            { return s.clientType }
func (s *evictionTestSession) Data() *session.SessionIdData          { return nil }
func (s *evictionTestSession) UserId() string                        { return s.userId }
func (s *evictionTestSession) UserData() json.RawMessage             { return nil }
func (s *evictionTestSession) ParsedUserData() (api.StringMap, error) { return nil, nil }
func (s *evictionTestSession) Backend() *talk.Backend                { return nil }
func (s *evictionTestSession) BackendUrl() string                    { return "" }
func (s *evictionTestSession) ParsedBackendUrl() *url.URL            { return nil }
func (s *evictionTestSession) SetRoom(room *Room, joinTime time.Time) {}
func (s *evictionTestSession) GetRoom() *Room                        { return nil }
func (s *evictionTestSession) IsInRoom(id string) bool               { return false }
func (s *evictionTestSession) LeaveRoom(notify bool) *Room           { return nil }
func (s *evictionTestSession) Close()                                {}
func (s *evictionTestSession) HasPermission(permission api.Permission) bool { return false }
func (s *evictionTestSession) SendError(e *api.Error) bool           { return false }
func (s *evictionTestSession) SendMessage(message *api.ServerMessage) bool { return false }

func TestRoom_SessionsToEvictForUser(t *testing.T) {
	t.Parallel()
	hub, asyncEvents, _, server := CreateHubForTestWithConfig(t, getTestConfig)
	defer server.Close()

	backend := hub.backend.GetBackend("default")
	require.NotNil(t, backend)
	room, err := NewRoom("testroom", nil, hub, asyncEvents, backend)
	require.NoError(t, err)

	oldSession := &evictionTestSession{publicId: "old", userId: "alice", clientType: api.HelloClientTypeClient}
	guestSession := &evictionTestSession{publicId: "guest", userId: "", clientType: api.HelloClientTypeClient}
	otherUser := &evictionTestSession{publicId: "bob", userId: "bob", clientType: api.HelloClientTypeClient}
	newSession := &evictionTestSession{publicId: "new", userId: "alice", clientType: api.HelloClientTypeClient}

	room.mu.Lock()
	room.inCallSessions[oldSession] = true
	room.inCallSessions[guestSession] = true
	room.inCallSessions[otherUser] = true
	room.mu.Unlock()

	evict := room.sessionsToEvictForUser(newSession)
	assert.Len(t, evict, 1)
	assert.Equal(t, api.PublicSessionId("old"), evict[0].PublicId())

	assert.Empty(t, room.sessionsToEvictForUser(guestSession))
}
