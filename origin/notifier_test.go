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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/clientqueue"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pelicanplatform/pelican/param"
	"github.com/pelicanplatform/pelican/server_structs"
	"github.com/pelicanplatform/pelican/server_utils"
	"github.com/pelicanplatform/pelican/test_utils"
)

var notifyEpoch = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// walkCall is one request the notifier made of the walker.
type walkCall struct {
	root  string
	depth int
}

// fakeWalker is an objectWalker over a table of object paths and modification
// times, recording what it was asked to walk.  Like the real one, it visits only
// objects within the depth it was given.
type fakeWalker struct {
	mu    sync.Mutex
	tree  map[string]time.Time
	calls []walkCall
	fail  map[string]error // roots whose walk fails
}

func newFakeWalker() *fakeWalker {
	return &fakeWalker{tree: map[string]time.Time{}, fail: map[string]error{}}
}

func (w *fakeWalker) set(objPath string, mtime time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tree[objPath] = mtime
}

func (w *fakeWalker) takeCalls() []walkCall {
	w.mu.Lock()
	defer w.mu.Unlock()
	calls := w.calls
	w.calls = nil
	return calls
}

func (w *fakeWalker) walk(ctx context.Context, root string, depth int, visit func(string, time.Time)) error {
	w.mu.Lock()
	w.calls = append(w.calls, walkCall{root, depth})
	err := w.fail[root]
	paths := make([]string, 0, len(w.tree))
	for objPath := range w.tree {
		paths = append(paths, objPath)
	}
	mtimes := make(map[string]time.Time, len(w.tree))
	for objPath, mtime := range w.tree {
		mtimes[objPath] = mtime
	}
	w.mu.Unlock()
	if err != nil {
		return err
	}

	sort.Strings(paths)
	for _, objPath := range paths {
		if levels, ok := levelsBelow(root, objPath); ok && levels <= depth {
			visit(objPath, mtimes[objPath])
		}
	}
	return nil
}

// notifierFixture is an objectEventSource over a fakeWalker, whose clock the test
// controls, exporting /ns.
type notifierFixture struct {
	t      *testing.T
	walker *fakeWalker
	source *objectEventSource
	clock  time.Time
}

func newNotifierFixture(t *testing.T, exportPrefixes ...string) *notifierFixture {
	t.Helper()
	t.Cleanup(test_utils.SetupTestLogging(t))
	server_utils.ResetTestState()
	t.Cleanup(server_utils.ResetTestState)
	require.NoError(t, param.Origin_EnableNotifications.Set(true))
	require.NoError(t, param.Origin_StorageType.Set("posix"))

	if len(exportPrefixes) == 0 {
		exportPrefixes = []string{"/ns"}
	}
	f := &notifierFixture{t: t, walker: newFakeWalker(), clock: notifyEpoch}
	f.source = newObjectEventSource(f.walker)
	f.source.now = func() time.Time { return f.clock }
	f.source.exports = func() ([]server_utils.OriginExport, error) {
		var exports []server_utils.OriginExport
		for _, prefix := range exportPrefixes {
			exports = append(exports, server_utils.OriginExport{
				FederationPrefix: prefix,
				Capabilities:     server_structs.Capabilities{Notify: true, Listings: true},
			})
		}
		return exports, nil
	}
	return f
}

// at moves the fixture's clock to notifyEpoch+offset.
func (f *notifierFixture) at(offset time.Duration) { f.clock = notifyEpoch.Add(offset) }

// subscribe registers a subscription as the notification server would, as of the
// fixture's clock.
func (f *notifierFixture) subscribe(clientID, params string) clientqueue.Queue {
	q := clientqueue.NewMemoryQueue(clientID, params)
	f.source.QueueAdded(q)
	return q
}

// walk runs one walk and returns the events it emitted.
func (f *notifierFixture) walk() []string {
	f.t.Helper()
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- f.source.walkOnce(ctx) }()

	var events []string
	for {
		select {
		case ev := <-f.source.events:
			events = append(events, ev)
		case err := <-done:
			require.NoError(f.t, err)
			return events
		}
	}
}

func TestParseSubscription(t *testing.T) {
	valid := []struct {
		params string
		region watchRegion
	}{
		{"2/osdf/ns/dir", watchRegion{"/ns/dir", 2}},
		{"1/pelican/ns/dir/", watchRegion{"/ns/dir", 1}},
		{"10/osdf/a", watchRegion{"/a", 10}},
		{"1/osdf/", watchRegion{"/", 1}},
	}
	for _, tc := range valid {
		t.Run(tc.params, func(t *testing.T) {
			region, err := parseSubscription(tc.params)
			require.NoError(t, err)
			assert.Equal(t, tc.region, region)
		})
	}

	invalid := []string{"", "osdf/ns/dir", "x/osdf/ns", "0/osdf/ns", "-1/osdf/ns", "1.5/osdf/ns", "2/osdf", "2"}
	for _, params := range invalid {
		t.Run("invalid "+params, func(t *testing.T) {
			_, err := parseSubscription(params)
			assert.Error(t, err)
		})
	}
}

func TestLevelsBelow(t *testing.T) {
	testCases := []struct {
		base, objPath string
		levels        int
		ok            bool
	}{
		{"/foo", "/foo", 0, true},
		{"/foo", "/foo/bar", 1, true},
		{"/foo", "/foo/bar/baz", 2, true},
		{"/foo", "/foobar", 0, false},
		{"/foo/bar", "/foo", 0, false},
		{"/", "/a", 1, true},
		{"/", "/a/b/c", 3, true},
		{"/", "/", 0, true},
	}
	for _, tc := range testCases {
		levels, ok := levelsBelow(tc.base, tc.objPath)
		assert.Equal(t, tc.ok, ok, "%s under %s", tc.objPath, tc.base)
		if tc.ok {
			assert.Equal(t, tc.levels, levels, "%s under %s", tc.objPath, tc.base)
		}
	}
}

func TestWatchRegionCovers(t *testing.T) {
	region := watchRegion{"/ns/dir", 2}
	assert.True(t, region.covers("/ns/dir/a"), "one level down")
	assert.True(t, region.covers("/ns/dir/a/b"), "two levels down")
	assert.False(t, region.covers("/ns/dir/a/b/c"), "beyond the depth")
	assert.True(t, region.covers("/ns/dir"), "the path itself, in case it is an object")
	assert.False(t, region.covers("/ns/dirty/a"), "whole segments only")
	assert.False(t, region.covers("/ns/other/a"))
	assert.True(t, watchRegion{"/", 1}.covers("/top"))
	assert.False(t, watchRegion{"/", 1}.covers("/a/b"))
}

func TestWatchRegionCoversRegion(t *testing.T) {
	assert.True(t, watchRegion{"/a", 3}.coversRegion(watchRegion{"/a", 3}))
	assert.True(t, watchRegion{"/a", 3}.coversRegion(watchRegion{"/a", 1}))
	assert.False(t, watchRegion{"/a", 1}.coversRegion(watchRegion{"/a", 3}))
	assert.True(t, watchRegion{"/a", 3}.coversRegion(watchRegion{"/a/b", 2}), "/a/b to depth 2 ends three levels below /a")
	assert.False(t, watchRegion{"/a", 3}.coversRegion(watchRegion{"/a/b", 3}), "but to depth 3 it ends four levels down")
	assert.False(t, watchRegion{"/a/b", 9}.coversRegion(watchRegion{"/a", 1}), "a region cannot cover its parent")
	assert.False(t, watchRegion{"/a", 9}.coversRegion(watchRegion{"/ab", 1}))
}

func TestRegionsFor(t *testing.T) {
	sub := func(path string, depth int) subscription { return subscription{watchRegion: watchRegion{path, depth}} }

	assert.Equal(t, []watchRegion{{"/ns/dir", 2}}, regionsFor("/ns", []subscription{sub("/ns/dir", 2)}),
		"a path inside the export is walked as it is")
	assert.Equal(t, []watchRegion{{"/ns", 1}}, regionsFor("/ns", []subscription{sub("/ns", 1)}))
	assert.Equal(t, []watchRegion{{"/ns", 2}}, regionsFor("/ns", []subscription{sub("/", 3)}),
		"a path above the export is walked from the export, with the levels down to it spent")
	assert.Equal(t, []watchRegion{{"/ns/a", 1}}, regionsFor("/ns/a", []subscription{sub("/ns", 2)}))
	assert.Empty(t, regionsFor("/ns", []subscription{sub("/", 1)}), "no depth left once the export is reached")
	assert.Empty(t, regionsFor("/ns", []subscription{sub("/other", 5)}), "another namespace")
	assert.Empty(t, regionsFor("/ns", []subscription{sub("/nsx", 5)}), "whole segments only")
}

func TestReduceRegions(t *testing.T) {
	assert.Equal(t, []watchRegion{{"/a", 3}},
		reduceRegions([]watchRegion{{"/a/b", 1}, {"/a", 3}, {"/a", 2}, {"/a", 3}}),
		"covered regions and duplicates are dropped")
	assert.Equal(t, []watchRegion{{"/a", 3}, {"/a/b", 3}},
		reduceRegions([]watchRegion{{"/a/b", 3}, {"/a", 3}}),
		"a deeper region below is not covered")
	assert.Equal(t, []watchRegion{{"/a", 1}, {"/b", 1}},
		reduceRegions([]watchRegion{{"/b", 1}, {"/a", 1}}), "unrelated regions stay, in order")
	assert.Empty(t, reduceRegions(nil))
}

func TestShouldNotifyAppliesTheSubscribersDepth(t *testing.T) {
	s := newObjectEventSource(newFakeWalker())
	queue := func(params string) clientqueue.Queue { return clientqueue.NewMemoryQueue("client", params) }

	assert.True(t, s.ShouldNotify("/ns/dir/obj", queue("1/osdf/ns/dir")))
	assert.False(t, s.ShouldNotify("/ns/dir/sub/obj", queue("1/osdf/ns/dir")), "two levels down is beyond depth 1")
	assert.True(t, s.ShouldNotify("/ns/dir/sub/obj", queue("2/osdf/ns/dir")))
	assert.True(t, s.ShouldNotify("/ns/dir/obj", queue("5/pelican/ns")))
	assert.False(t, s.ShouldNotify("/ns/dirty/obj", queue("5/osdf/ns/dir")), "a sibling that merely shares a prefix")
	assert.False(t, s.ShouldNotify("/ns/other/obj", queue("5/osdf/ns/dir")))
	assert.False(t, s.ShouldNotify("/ns/dir/obj", queue("osdf/ns/dir")), "no depth")
	assert.False(t, s.ShouldNotify("/ns/dir/obj", queue("not-a-subscription")))
}

func TestWalkOnlyWalksWhatIsSubscribed(t *testing.T) {
	f := newNotifierFixture(t)
	f.walker.set("/ns/dir/a", notifyEpoch)
	f.walker.set("/ns/elsewhere/b", notifyEpoch)

	assert.Empty(t, f.walk())
	assert.Empty(t, f.walker.takeCalls(), "with no subscribers nothing is walked")

	q := f.subscribe("c1", "2/osdf/ns/dir")
	f.walk()
	assert.Equal(t, []walkCall{{"/ns/dir", 2}}, f.walker.takeCalls(), "only the subscribed path, to its depth")

	f.source.QueueRemoved(q)
	f.walk()
	assert.Empty(t, f.walker.takeCalls(), "and nothing again once the subscriber leaves")
}

func TestWalkIgnoresSubscriptionsOutsideTheExports(t *testing.T) {
	f := newNotifierFixture(t, "/ns")
	f.subscribe("c1", "2/osdf/other/dir")
	f.subscribe("c2", "x/osdf/ns/dir") // malformed, never registered
	f.walk()
	assert.Empty(t, f.walker.takeCalls())
}

func TestNewSubscriptionHearsOnlyOfObjectsNewerThanItself(t *testing.T) {
	f := newNotifierFixture(t)
	f.walker.set("/ns/dir/before", notifyEpoch.Add(5*time.Minute))
	f.at(10 * time.Minute)
	f.subscribe("c1", "1/osdf/ns/dir")
	f.walker.set("/ns/dir/after", notifyEpoch.Add(11*time.Minute))

	assert.Equal(t, []string{"/ns/dir/after"}, f.walk(), "what was already there when it subscribed is not news")
	assert.Empty(t, f.walk(), "nor is anything announced twice")
}

func TestWalkAnnouncesCreationsAndModifications(t *testing.T) {
	f := newNotifierFixture(t)
	f.walker.set("/ns/dir/existing", notifyEpoch.Add(time.Minute))
	f.at(2 * time.Minute)
	f.subscribe("c1", "3/osdf/ns/dir")
	require.Empty(t, f.walk())

	f.walker.set("/ns/dir/sub/new", notifyEpoch.Add(3*time.Minute))
	f.walker.set("/ns/dir/existing", notifyEpoch.Add(4*time.Minute))
	assert.Equal(t, []string{"/ns/dir/sub/new", "/ns/dir/existing"}, f.walk(), "federation paths, oldest change first")
	assert.Empty(t, f.walk())
}

func TestWalkDoesNotAnnounceDeletions(t *testing.T) {
	f := newNotifierFixture(t)
	f.walker.set("/ns/dir/gone", notifyEpoch.Add(time.Minute))
	f.at(2 * time.Minute)
	f.subscribe("c1", "1/osdf/ns/dir")
	require.Empty(t, f.walk())

	f.walker.mu.Lock()
	delete(f.walker.tree, "/ns/dir/gone")
	f.walker.mu.Unlock()
	assert.Empty(t, f.walk())
}

func TestWalkRespectsDepth(t *testing.T) {
	f := newNotifierFixture(t)
	f.subscribe("c1", "2/osdf/ns/dir")
	f.at(time.Minute)
	f.walker.set("/ns/dir/l1", notifyEpoch.Add(2*time.Minute))
	f.walker.set("/ns/dir/a/l2", notifyEpoch.Add(3*time.Minute))
	f.walker.set("/ns/dir/a/b/l3", notifyEpoch.Add(4*time.Minute))

	assert.Equal(t, []string{"/ns/dir/l1", "/ns/dir/a/l2"}, f.walk(), "level 3 is below the depth subscribed to")
}

func TestDeeperSubscriptionStartsAfresh(t *testing.T) {
	f := newNotifierFixture(t)
	f.subscribe("c1", "1/osdf/ns/dir")
	f.at(time.Minute)
	f.walker.set("/ns/dir/l1", notifyEpoch.Add(2*time.Minute))
	f.walker.set("/ns/dir/a/l2", notifyEpoch.Add(90*time.Second)) // already there when c2 arrives
	require.Equal(t, []string{"/ns/dir/l1"}, f.walk())

	// A second client wants one more level.  Level two was not being walked, so
	// what is there is old news to it, but anything newer is not.
	f.at(3 * time.Minute)
	f.subscribe("c2", "2/osdf/ns/dir")
	f.walker.set("/ns/dir/a/l2-new", notifyEpoch.Add(4*time.Minute))
	f.walker.set("/ns/dir/l1", notifyEpoch.Add(5*time.Minute))
	assert.Equal(t, []string{"/ns/dir/a/l2-new", "/ns/dir/l1"}, f.walk())
	assert.Equal(t, []walkCall{{"/ns/dir", 1}, {"/ns/dir", 2}}, f.walker.takeCalls(), "one walk of the deepest, not one per subscriber")
}

func TestOverlappingSubscriptionsAreWalkedOnceAndAnnouncedOnce(t *testing.T) {
	f := newNotifierFixture(t)
	f.subscribe("c1", "3/osdf/ns/a")
	f.subscribe("c2", "1/osdf/ns/a/b")
	f.subscribe("c3", "3/osdf/ns/a/b") // goes deeper than c1 does there
	f.at(time.Minute)
	f.walker.set("/ns/a/b/c/d", notifyEpoch.Add(2*time.Minute))

	assert.Equal(t, []string{"/ns/a/b/c/d"}, f.walk())
	assert.ElementsMatch(t, []walkCall{{"/ns/a", 3}, {"/ns/a/b", 3}}, f.walker.takeCalls())
}

func TestSubscriptionAboveTheExportIsWalkedFromTheExport(t *testing.T) {
	f := newNotifierFixture(t, "/ns", "/other")
	f.subscribe("c1", "3/osdf/")
	f.at(time.Minute)
	f.walker.set("/ns/a/obj", notifyEpoch.Add(2*time.Minute))
	f.walker.set("/other/obj", notifyEpoch.Add(3*time.Minute))
	f.walker.set("/ns/a/b/obj", notifyEpoch.Add(4*time.Minute)) // three levels below /ns: beyond depth 3 from /

	assert.Equal(t, []string{"/ns/a/obj", "/other/obj"}, f.walk())
	assert.ElementsMatch(t, []walkCall{{"/ns", 2}, {"/other", 2}}, f.walker.takeCalls())
}

func TestFailedWalkIsTreatedAsNewNextTime(t *testing.T) {
	f := newNotifierFixture(t, "/ns", "/other")
	f.subscribe("c1", "1/osdf/ns")
	f.subscribe("c2", "1/osdf/other")
	f.at(time.Minute)
	f.walker.set("/ns/obj", notifyEpoch.Add(2*time.Minute))
	f.walker.set("/other/obj", notifyEpoch.Add(2*time.Minute))
	f.walker.fail["/ns"] = assert.AnError

	assert.Equal(t, []string{"/other/obj"}, f.walk(), "one region failing does not hide the other")

	delete(f.walker.fail, "/ns")
	f.walker.set("/other/obj", notifyEpoch.Add(3*time.Minute))
	assert.ElementsMatch(t, []string{"/ns/obj", "/other/obj"}, f.walk(),
		"the region that failed is walked as new, so what changed meanwhile is announced")
}

func TestRunWalksPeriodicallyAndClosesOnCancel(t *testing.T) {
	f := newNotifierFixture(t)
	f.source.interval = time.Millisecond
	f.source.now = time.Now
	f.subscribe("c1", "1/osdf/ns/dir")

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		f.source.run(ctx)
		close(runDone)
	}()

	// Objects written after the subscription arrive on a later tick.
	f.walker.set("/ns/dir/b", time.Now().Add(time.Minute))
	select {
	case ev := <-f.source.Events():
		assert.Equal(t, "/ns/dir/b", ev)
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

func TestExportSupportsNotify(t *testing.T) {
	t.Cleanup(test_utils.SetupTestLogging(t))
	server_utils.ResetTestState()
	t.Cleanup(server_utils.ResetTestState)

	export := func(notify, listings bool) server_utils.OriginExport {
		return server_utils.OriginExport{
			FederationPrefix: "/ns",
			Capabilities:     server_structs.Capabilities{Notify: notify, Listings: listings},
		}
	}
	configure := func(enabled bool, storageType string) {
		require.NoError(t, param.Origin_EnableNotifications.Set(enabled))
		require.NoError(t, param.Origin_StorageType.Set(storageType))
	}

	for _, storageType := range []string{"posix", "s3", "https", "xroot"} {
		configure(true, storageType)
		assert.True(t, exportSupportsNotify(export(true, true)), storageType)
	}
	for _, storageType := range []string{"posixv2", "s3v2", "httpsv2", "globusv2", "pstore", "ssh"} {
		configure(true, storageType)
		assert.False(t, exportSupportsNotify(export(true, true)), "native backend %s is not supported", storageType)
	}

	configure(true, "posix")
	assert.False(t, exportSupportsNotify(export(false, true)), "the export lacks the Notify capability")
	assert.False(t, exportSupportsNotify(export(true, false)), "the export cannot be listed")
	configure(false, "posix")
	assert.False(t, exportSupportsNotify(export(true, true)), "notifications are switched off")
}

// Debug logging explains what the notifier is doing: each walk, each new object
// it finds, and for every subscriber whether that object is theirs.
func TestNotifierDebugLogging(t *testing.T) {
	f := newNotifierFixture(t)

	previousLevel := logrus.GetLevel()
	logrus.SetLevel(logrus.DebugLevel)
	t.Cleanup(func() { logrus.SetLevel(previousLevel) })
	hook := &logtest.Hook{}
	logrus.AddHook(hook)

	messages := func() []string {
		var out []string
		for _, entry := range hook.AllEntries() {
			if strings.HasPrefix(entry.Message, "Object notifier:") {
				out = append(out, entry.Message)
			}
		}
		return out
	}

	assert.Empty(t, f.walk())
	require.Len(t, messages(), 1)
	assert.Contains(t, messages()[0], "no subscribers")

	mine := f.subscribe("a", "2/osdf/ns/dir")
	other := f.subscribe("b", "2/osdf/ns/elsewhere")
	f.at(time.Minute)
	f.walker.set("/ns/dir/new", notifyEpoch.Add(2*time.Minute))
	before := len(messages())
	require.Equal(t, []string{"/ns/dir/new"}, f.walk())
	walk := messages()[before:]
	require.Len(t, walk, 3, "start, one new object, end: %v", walk)
	assert.Contains(t, walk[0], "starting walk for 2 subscriber(s)")
	assert.Contains(t, walk[1], `found new or modified object "/ns/dir/new"`)
	assert.Contains(t, walk[2], "1 new or modified object(s)")

	// The notification server asks about every subscriber for each new object.
	before = len(messages())
	require.True(t, f.source.ShouldNotify("/ns/dir/new", mine))
	require.False(t, f.source.ShouldNotify("/ns/dir/new", other))
	decisions := messages()[before:]
	require.Len(t, decisions, 2)
	assert.Contains(t, decisions[0], `new object "/ns/dir/new"`)
	assert.Contains(t, decisions[0], `"2/osdf/ns/dir"`)
	assert.Contains(t, decisions[0], "notify=true")
	assert.Contains(t, decisions[1], `"2/osdf/ns/elsewhere"`)
	assert.Contains(t, decisions[1], "notify=false")
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
// that subscribes to a directory with a depth hears of objects created beneath it
// within that depth -- and only those.  Nothing else is even walked.
func TestObjectNotifierEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	f := newNotifierFixture(t)
	f.source.interval = time.Millisecond
	f.source.now = time.Now

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

	// The destination is "<client id>/<depth>/<protocol>/<object path>".
	sendStompFrame(t, conn, "SUBSCRIBE", map[string]string{
		"id":           "0",
		"destination":  "client1/2/osdf/ns/dir",
		"subscription": "client1",
		"ack":          "client-individual",
		"receipt":      "subscribed",
	})
	for frame := readStompFrame(t, conn); frame.command != "RECEIPT"; frame = readStompFrame(t, conn) {
		require.NotEqual(t, "ERROR", frame.command, frame.body)
	}

	// Objects written before the subscription was registered are not news, so keep
	// writing newer ones until the subscriber has been registered and one arrives.
	// Objects outside the subscribed path and depth must never be delivered.
	var delivered []string
	require.Eventually(t, func() bool {
		now := time.Now().Add(time.Minute)
		f.walker.set("/ns/dir/sub/new", now)
		f.walker.set("/ns/dir/sub/too/deep", now)
		f.walker.set("/ns/elsewhere/obj", now)
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
		var objPath string
		require.NoError(t, json.Unmarshal([]byte(strings.TrimSuffix(body, "\x00")), &objPath))
		delivered = append(delivered, objPath)
		return len(delivered) >= 3
	}, 20*time.Second, 10*time.Millisecond)
	for _, objPath := range delivered {
		assert.Equal(t, "/ns/dir/sub/new", objPath)
	}
	for _, call := range f.walker.takeCalls() {
		assert.Equal(t, walkCall{"/ns/dir", 2}, call, "only the subscribed path was walked")
	}
}
