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

package origin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/clientqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/webdav"

	"github.com/pelicanplatform/pelican/server_structs"
	"github.com/pelicanplatform/pelican/server_utils"
	"github.com/pelicanplatform/pelican/test_utils"
)

// notifierFixture is an objectEventSource over a temporary directory exported
// at /ns, with every file's modification time under the test's control.
type notifierFixture struct {
	t         *testing.T
	root      string
	stateFile string
	source    *objectEventSource
}

var fixtureEpoch = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newNotifierFixture(t *testing.T) *notifierFixture {
	t.Cleanup(test_utils.SetupTestLogging(t))
	root := t.TempDir()
	f := &notifierFixture{t: t, root: root, stateFile: filepath.Join(t.TempDir(), "state")}
	f.source = f.newSource()
	return f
}

// newSource builds a fresh source on the same files and state file, as a restart would.
func (f *notifierFixture) newSource() *objectEventSource {
	s := newObjectEventSource(f.stateFile)
	s.exports = func() ([]server_utils.OriginExport, error) {
		return []server_utils.OriginExport{{FederationPrefix: "/ns", Capabilities: server_structs.Capabilities{Notify: true}}}, nil
	}
	s.fsFor = func(server_utils.OriginExport) webdav.FileSystem { return webdav.Dir(f.root) }
	return s
}

// write creates or updates a file, giving it the modification time fixtureEpoch+age.
func (f *notifierFixture) write(rel, content string, age time.Duration) {
	f.t.Helper()
	full := filepath.Join(f.root, rel)
	require.NoError(f.t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(f.t, os.WriteFile(full, []byte(content), 0o644))
	mtime := fixtureEpoch.Add(age)
	require.NoError(f.t, os.Chtimes(full, mtime, mtime))
}

// walk runs one walk and returns the events it emitted.
func (f *notifierFixture) walk(s *objectEventSource) []string {
	f.t.Helper()
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- s.walkOnce(ctx) }()

	var events []string
	for {
		select {
		case ev := <-s.events:
			events = append(events, ev)
		case err := <-done:
			require.NoError(f.t, err)
			return events
		}
	}
}

func TestWalkFirstRunOnlyRecordsABaseline(t *testing.T) {
	f := newNotifierFixture(t)
	f.write("old1", "a", time.Minute)
	f.write("dir/old2", "b", 2*time.Minute)

	assert.Empty(t, f.walk(f.source), "pre-existing objects must not be announced")

	stored, ok, err := f.source.state.load()
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, stored.Equal(fixtureEpoch.Add(2*time.Minute)), "the baseline is the newest mtime seen, got %v", stored)
}

func TestWalkAnnouncesCreationsAndModifications(t *testing.T) {
	f := newNotifierFixture(t)
	f.write("existing", "a", time.Minute)
	f.walk(f.source) // baseline

	f.write("dir/sub/new", "n", 2*time.Minute)
	f.write("existing", "a changed", 3*time.Minute)
	events := f.walk(f.source)
	assert.Equal(t, []string{"/ns/dir/sub/new", "/ns/existing"}, events, "federation paths, oldest change first")

	assert.Empty(t, f.walk(f.source), "an unchanged store announces nothing")
}

func TestWalkDoesNotAnnounceDeletionsOrDirectories(t *testing.T) {
	f := newNotifierFixture(t)
	f.write("gone", "a", time.Minute)
	f.write("kept", "a", time.Minute)
	f.walk(f.source) // baseline

	require.NoError(t, os.Remove(filepath.Join(f.root, "gone")))
	// A new, empty directory is not an object.
	require.NoError(t, os.Mkdir(filepath.Join(f.root, "emptydir"), 0o755))
	require.NoError(t, os.Chtimes(filepath.Join(f.root, "emptydir"), fixtureEpoch.Add(time.Hour), fixtureEpoch.Add(time.Hour)))

	assert.Empty(t, f.walk(f.source))
}

func TestWalkEmptyStoreBaselineStillCatchesLaterObjects(t *testing.T) {
	f := newNotifierFixture(t)
	assert.Empty(t, f.walk(f.source))

	// Written "now", as a real object would be; the empty-store baseline is the
	// wall clock at the first walk.
	f.write("first", "a", time.Since(fixtureEpoch)+time.Minute)
	assert.Equal(t, []string{"/ns/first"}, f.walk(f.source))
}

func TestWalkStateSurvivesRestart(t *testing.T) {
	f := newNotifierFixture(t)
	f.write("a", "a", time.Minute)
	f.walk(f.source) // baseline
	f.write("b", "b", 2*time.Minute)
	require.Equal(t, []string{"/ns/b"}, f.walk(f.source))

	// A restarted origin neither repeats what it announced...
	restarted := f.newSource()
	assert.Empty(t, f.walk(restarted))

	// ...nor loses what was written while it was down.
	f.write("c", "c", 3*time.Minute)
	restarted = f.newSource()
	assert.Equal(t, []string{"/ns/c"}, f.walk(restarted))
}

func TestWalkSkipsExportsWithoutABackend(t *testing.T) {
	f := newNotifierFixture(t)
	f.write("a", "a", time.Minute)
	f.walk(f.source) // baseline
	f.write("b", "b", 2*time.Minute)

	f.source.fsFor = func(server_utils.OriginExport) webdav.FileSystem { return nil }
	assert.Empty(t, f.walk(f.source))
}

func TestWalkRejectsMalformedState(t *testing.T) {
	f := newNotifierFixture(t)
	require.NoError(t, os.WriteFile(f.stateFile, []byte("not a time"), 0o644))
	err := f.source.walkOnce(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "malformed notifier state")
}

func TestRunWalksPeriodicallyAndClosesOnCancel(t *testing.T) {
	f := newNotifierFixture(t)
	f.write("a", "a", time.Minute)
	f.source.interval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		f.source.run(ctx)
		close(runDone)
	}()

	// The first walk records the baseline; a later object arrives on a later tick.
	require.Eventually(t, func() bool {
		_, ok, _ := f.source.state.load()
		return ok
	}, 5*time.Second, time.Millisecond)
	f.write("b", "b", 2*time.Minute)
	select {
	case ev := <-f.source.Events():
		assert.Equal(t, "/ns/b", ev)
	case <-time.After(5 * time.Second):
		t.Fatal("no event for the new object")
	}

	cancel()
	require.Eventually(t, func() bool {
		select {
		case <-runDone:
			return true
		default:
			return false
		}
	}, 5*time.Second, time.Millisecond)
	_, open := <-f.source.Events()
	assert.False(t, open, "the events channel is closed when the notifier stops")
}

func TestSubscribedPath(t *testing.T) {
	testCases := []struct {
		params   string
		expected string
		ok       bool
	}{
		{"osdf/vdc/public", "/vdc/public", true},
		{"pelican/ns/dir/", "/ns/dir", true},
		{"osdf/", "/", true},
		{"osdf", "", false},
		{"", "", false},
	}
	for _, tc := range testCases {
		t.Run(tc.params, func(t *testing.T) {
			got, ok := subscribedPath(tc.params)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestPathCovers(t *testing.T) {
	assert.True(t, pathCovers("/foo", "/foo"))
	assert.True(t, pathCovers("/foo", "/foo/bar"))
	assert.True(t, pathCovers("/foo", "/foo/bar/baz"))
	assert.False(t, pathCovers("/foo", "/foobar"), "whole segments only")
	assert.False(t, pathCovers("/foo/bar", "/foo"))
	assert.True(t, pathCovers("/", "/anything/at/all"))
}

func TestShouldNotify(t *testing.T) {
	s := newObjectEventSource(filepath.Join(t.TempDir(), "state"))
	queue := func(params string) clientqueue.Queue { return clientqueue.NewMemoryQueue("client", params) }

	assert.True(t, s.ShouldNotify("/ns/dir/obj", queue("osdf/ns/dir")))
	assert.True(t, s.ShouldNotify("/ns/dir/obj", queue("pelican/ns")))
	assert.False(t, s.ShouldNotify("/ns/other/obj", queue("osdf/ns/dir")))
	assert.False(t, s.ShouldNotify("/ns/dirty/obj", queue("osdf/ns/dir")), "a sibling that merely shares a prefix")
	assert.False(t, s.ShouldNotify("/ns/dir/obj", queue("not-a-subscription")))
}

func TestExportSupportsNotifyNeedsACapabilityAndABackend(t *testing.T) {
	withCap := server_utils.OriginExport{FederationPrefix: "/no-backend", Capabilities: server_structs.Capabilities{Notify: true}}
	without := server_utils.OriginExport{FederationPrefix: "/no-backend"}
	assert.False(t, exportSupportsNotify(withCap), "no registered backend for the prefix")
	assert.False(t, exportSupportsNotify(without))
}

// stompFrame is one decoded STOMP frame.
type stompFrame struct {
	command string
	headers map[string]string
	body    string
}

func sendStompFrame(t *testing.T, conn *websocket.Conn, command string, headers map[string]string) {
	t.Helper()
	frame := command + "\n"
	for k, v := range headers {
		frame += k + ":" + v + "\n"
	}
	frame += "\n\x00"
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(frame)))
}

// readStompFrame returns the next frame, skipping heartbeats.
func readStompFrame(t *testing.T, conn *websocket.Conn) stompFrame {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
	for {
		_, data, err := conn.ReadMessage()
		require.NoError(t, err)
		text := strings.TrimLeft(string(data), "\n")
		if text == "" {
			continue // heartbeat
		}
		head, body, _ := strings.Cut(text, "\n\n")
		lines := strings.Split(head, "\n")
		frame := stompFrame{command: lines[0], headers: map[string]string{}, body: strings.TrimSuffix(body, "\x00")}
		for _, line := range lines[1:] {
			k, v, _ := strings.Cut(line, ":")
			frame.headers[k] = v
		}
		return frame
	}
}

// The notifier is served at its route on the origin's web server, and a client
// that subscribes to a directory hears of objects created beneath it -- and only
// those.
func TestObjectNotifierEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	f := newNotifierFixture(t)
	f.write("dir/existing", "x", time.Minute)
	f.source.interval = time.Millisecond

	engine := gin.New()
	require.NoError(t, mountObjectNotifier(engine, f.source))
	server := httptest.NewServer(engine)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		f.source.run(ctx)
		close(runDone)
	}()
	defer func() {
		cancel()
		<-runDone
	}()

	// The bare route, with no trailing slash, must upgrade without a redirect.
	wsUrl := "ws" + strings.TrimPrefix(server.URL, "http") + server_structs.OriginNotifyRoute
	conn, resp, err := websocket.DefaultDialer.Dial(wsUrl, nil)
	require.NoError(t, err)
	defer conn.Close()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	sendStompFrame(t, conn, "CONNECT", map[string]string{"accept-version": "1.2", "host": "origin"})
	require.Equal(t, "CONNECTED", readStompFrame(t, conn).command)

	// The destination is "<client id>/<protocol>/<object path>".
	sendStompFrame(t, conn, "SUBSCRIBE", map[string]string{
		"id":           "0",
		"destination":  "client1/osdf/ns/dir",
		"subscription": "client1",
		"ack":          "client-individual",
		"receipt":      "subscribed",
	})
	for frame := readStompFrame(t, conn); frame.command != "RECEIPT"; frame = readStompFrame(t, conn) {
		require.NotEqual(t, "ERROR", frame.command, frame.body)
	}

	// Events produced before a queue exists are dropped, so keep writing until the
	// subscriber has been registered and one arrives.  The notification server
	// delivers each event as a JSON string.  Objects outside the subscribed
	// directory must never be delivered.
	var delivered []string
	require.Eventually(t, func() bool {
		f.write("elsewhere/obj", "x", time.Since(fixtureEpoch))
		f.write("dir/sub/new", "x", time.Since(fixtureEpoch))
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(50*time.Millisecond)))
		_, data, err := conn.ReadMessage()
		if err != nil {
			return len(delivered) > 0
		}
		text := strings.TrimLeft(string(data), "\n")
		if !strings.HasPrefix(text, "MESSAGE") {
			return len(delivered) > 0
		}
		_, body, _ := strings.Cut(text, "\n\n")
		var path string
		require.NoError(t, json.Unmarshal([]byte(strings.TrimSuffix(body, "\x00")), &path))
		delivered = append(delivered, path)
		return len(delivered) >= 3
	}, 20*time.Second, 10*time.Millisecond)
	for _, path := range delivered {
		assert.Equal(t, "/ns/dir/sub/new", path)
	}
}
