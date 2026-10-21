/***************************************************************
 *
 * Copyright (C) 2026, Pelican Project, Morgridge Institute for Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***************************************************************/

package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/eventsource"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/notificationserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pelicanplatform/pelican/param"
	"github.com/pelicanplatform/pelican/test_utils"
)

// pushSource is an eventsource.EventSource that announces whatever the test
// pushes into it.
type pushSource struct {
	eventsource.DefaultNotifier
	events chan string
}

func (s *pushSource) Events() <-chan string { return s.events }

// notifyFixture serves an origin's notifications over TLS, as origins do.
type notifyFixture struct {
	t      *testing.T
	server *httptest.Server
	source *pushSource
}

func newNotifyFixture(t *testing.T) *notifyFixture {
	t.Helper()
	test_utils.InitClient(t, map[param.Param]any{param.TLSSkipVerify: true})
	f := &notifyFixture{t: t, source: &pushSource{events: make(chan string)}}
	handler, err := notificationserver.New(notificationserver.NotificationServerArgs{EventSource: f.source})
	require.NoError(t, err)
	f.server = httptest.NewTLSServer(handler)
	t.Cleanup(f.server.Close)
	return f
}

func (f *notifyFixture) result(clientID, topic string) SubscribeResult {
	return SubscribeResult{
		NotifyUrl: "wss" + strings.TrimPrefix(f.server.URL, "https"),
		ClientID:  clientID,
		Topic:     topic,
	}
}

// push announces an event, giving up if the notification server is not reading.
func (f *notifyFixture) push(event string) {
	select {
	case f.source.events <- event:
	case <-time.After(time.Second):
	}
}

// listen runs Listen in the background, collecting what it hears.
type listener struct {
	mu       sync.Mutex
	heard    []Notification
	done     chan error
	cancel   context.CancelFunc
	finished bool
}

func (l *listener) notifications() []Notification {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Notification(nil), l.heard...)
}

func startListening(t *testing.T, r SubscribeResult) *listener {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	l := &listener{done: make(chan error, 1), cancel: cancel}
	t.Cleanup(cancel)
	go func() {
		l.done <- r.Listen(ctx, func(n Notification) {
			l.mu.Lock()
			l.heard = append(l.heard, n)
			l.mu.Unlock()
		})
	}()
	return l
}

func TestListenReceivesAndAcknowledgesNotifications(t *testing.T) {
	f := newNotifyFixture(t)
	l := startListening(t, f.result("c1", "c1/1/pelican/ns"))

	// Events pushed before the subscription exists are dropped, so keep announcing
	// until the first arrives.  The origin sends the next event only once the
	// previous one has been acknowledged, so hearing a second shows that Listen acks.
	require.Eventually(t, func() bool {
		f.push("/ns/a")
		return len(l.notifications()) >= 1
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		f.push("/ns/b")
		for _, n := range l.notifications() {
			if n.Path == "/ns/b" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)

	first := l.notifications()[0]
	assert.Equal(t, "/ns/a", first.Path, "the JSON string the server sends is decoded to the path")
	assert.WithinDuration(t, time.Now(), first.ReceivedAt, time.Minute)

	l.cancel()
	select {
	case err := <-l.done:
		assert.NoError(t, err, "cancelling the context is a clean stop")
	case <-time.After(10 * time.Second):
		t.Fatal("Listen did not return after its context was cancelled")
	}
}

// fakeStompOrigin is a websocket server that speaks just enough STOMP to accept a
// client's CONNECT and SUBSCRIBE, and then does what afterSubscribe says.  It
// returns the endpoint's wss URL.
func fakeStompOrigin(t *testing.T, afterSubscribe func(ws *websocket.Conn, subscribe string)) string {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				return
			}
			switch {
			case strings.HasPrefix(string(data), "CONNECT"), strings.HasPrefix(string(data), "STOMP"):
				_ = ws.WriteMessage(websocket.TextMessage, []byte("CONNECTED\nversion:1.2\n\n\x00"))
			case strings.HasPrefix(string(data), "SUBSCRIBE"):
				afterSubscribe(ws, string(data))
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	return "wss" + strings.TrimPrefix(server.URL, "https")
}

func listenToFake(t *testing.T, endpoint string) error {
	t.Helper()
	test_utils.InitClient(t, map[param.Param]any{param.TLSSkipVerify: true})
	done := make(chan error, 1)
	go func() {
		done <- SubscribeResult{NotifyUrl: endpoint, ClientID: "c1", Topic: "c1/1/pelican/ns"}.
			Listen(context.Background(), func(Notification) {})
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Listen did not return")
		return nil
	}
}

func TestListenSubscribesWithTheTopicAndClientID(t *testing.T) {
	subscribe := make(chan string, 1)
	endpoint := fakeStompOrigin(t, func(ws *websocket.Conn, frame string) {
		subscribe <- frame
		_ = ws.Close()
	})
	_ = listenToFake(t, endpoint)

	frame := <-subscribe
	assert.True(t, strings.HasPrefix(frame, "SUBSCRIBE\n"), frame)
	assert.Contains(t, frame, "destination:c1/1/pelican/ns", "the topic is the destination")
	assert.Contains(t, frame, "subscription:c1", "the origin keys the subscription's queue on this header")
	assert.Contains(t, frame, "id:c1")
	assert.Contains(t, frame, "ack:client-individual", "each notification is acknowledged on its own")
}

func TestListenReportsAnErrorTheOriginSends(t *testing.T) {
	endpoint := fakeStompOrigin(t, func(ws *websocket.Conn, _ string) {
		_ = ws.WriteMessage(websocket.TextMessage,
			[]byte("ERROR\nmessage:queue exceeded 100 events\ncontent-type:text/plain\n\nqueue exceeded 100 events\x00"))
	})
	err := listenToFake(t, endpoint)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "c1/1/pelican/ns", "which subscription ended")
	assert.Contains(t, err.Error(), "ended")
}

func TestListenReportsAConnectionTheOriginDrops(t *testing.T) {
	endpoint := fakeStompOrigin(t, func(ws *websocket.Conn, _ string) { _ = ws.Close() })
	err := listenToFake(t, endpoint)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ended")
}

func TestListenReportsAnUnreachableOrigin(t *testing.T) {
	test_utils.InitClient(t, map[param.Param]any{param.TLSSkipVerify: true})
	// Nothing listens here: a server that was started and closed.
	closed := httptest.NewTLSServer(nil)
	address := strings.TrimPrefix(closed.URL, "https")
	closed.Close()

	err := SubscribeResult{NotifyUrl: "wss" + address, ClientID: "c1", Topic: "c1/1/pelican/ns"}.
		Listen(context.Background(), func(Notification) {})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect")
}

func TestListenNeedsAnEndpointATopicAndAClientID(t *testing.T) {
	noop := func(Notification) {}
	for _, r := range []SubscribeResult{
		{ClientID: "c", Topic: "c/1/pelican/ns"},
		{NotifyUrl: "wss://origin.example.com/x", Topic: "c/1/pelican/ns"},
		{NotifyUrl: "wss://origin.example.com/x", ClientID: "c"},
	} {
		assert.Error(t, r.Listen(context.Background(), noop))
	}
}

func TestNotifiedPath(t *testing.T) {
	assert.Equal(t, "/ns/dir/obj", notifiedPath([]byte(`"/ns/dir/obj"`)), "the server sends a JSON string")
	assert.Equal(t, "/with space/and \"quote\"", notifiedPath([]byte(`"/with space/and \"quote\""`)))
	assert.Equal(t, "/ns/plain", notifiedPath([]byte("/ns/plain")), "anything else is kept as it is")
}
