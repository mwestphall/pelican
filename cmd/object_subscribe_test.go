//go:build client

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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/eventsource"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/notificationserver"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pelicanplatform/pelican/client"
	"github.com/pelicanplatform/pelican/param"
	"github.com/pelicanplatform/pelican/server_structs"
	"github.com/pelicanplatform/pelican/test_utils"
)

func TestSubscribeFlags(t *testing.T) {
	verbose := subscribeCmd.Flags().Lookup("verbose")
	require.NotNil(t, verbose)
	assert.Equal(t, "v", verbose.Shorthand)
	assert.Equal(t, "false", verbose.DefValue)

	depth := subscribeCmd.Flags().Lookup("depth")
	require.NotNil(t, depth)
	assert.Equal(t, "1", depth.DefValue)

	noListen := subscribeCmd.Flags().Lookup("no-listen")
	require.NotNil(t, noListen)
	assert.Equal(t, "false", noListen.DefValue)

	require.NotNil(t, subscribeCmd.Flags().Lookup("client-id"))
	require.NotNil(t, subscribeCmd.Flags().Lookup("json"))
}

func TestWriteSubscriptionDetails(t *testing.T) {
	result := client.SubscribeResult{
		Namespace: "/ns",
		NotifyUrl: "wss://origin.example.com:8444/api/v1.0/origin/notify",
		Origins:   []string{"https://origin.example.com:8443", "https://origin2.example.com:8443"},
		ClientID:  "client1",
		Depth:     2,
		Topic:     "client1/2/osdf/ns/dir",
	}

	var out bytes.Buffer
	writeSubscriptionDetails(&out, result, true)
	text := out.String()

	assert.Contains(t, text, "Endpoint:            wss://origin.example.com:8444/api/v1.0/origin/notify", "the connection endpoint")
	assert.Contains(t, text, "Origins:             https://origin.example.com:8443, https://origin2.example.com:8443")
	assert.Contains(t, text, "Namespace:           /ns")
	assert.Contains(t, text, "Client ID:           client1")
	assert.Contains(t, text, "Depth:               2")
	assert.Contains(t, text, "Topic:               client1/2/osdf/ns/dir")
	assert.Contains(t, text, "Subscription params: 2/osdf/ns/dir", "what the origin receives: the topic without the client id")
	assert.Contains(t, text, "TLS verification:    true")
	assert.Contains(t, text, "Acknowledgement:     client-individual")

	out.Reset()
	writeSubscriptionDetails(&out, result, false)
	assert.Contains(t, out.String(), "TLS verification:    false")
}

func TestWriteNotification(t *testing.T) {
	n := client.Notification{Path: "/ns/dir/obj", ReceivedAt: time.Date(2026, 1, 2, 3, 4, 5, 600, time.UTC)}

	var out bytes.Buffer
	require.NoError(t, writeNotification(&out, n, false))
	assert.Equal(t, "/ns/dir/obj\n", out.String(), "one path per line")

	out.Reset()
	require.NoError(t, writeNotification(&out, n, true))
	assert.True(t, strings.HasSuffix(out.String(), "\n"))
	var decoded struct {
		Path       string `json:"path"`
		ReceivedAt string `json:"receivedAt"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
	assert.Equal(t, "/ns/dir/obj", decoded.Path)
	assert.Equal(t, "2026-01-02T03:04:05.0000006Z", decoded.ReceivedAt)

	out.Reset()
	require.NoError(t, writeNotification(&out, client.Notification{Path: "/odd \"name\"\n"}, true))
	require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
	assert.Equal(t, "/odd \"name\"\n", decoded.Path, "names with quotes and newlines survive as JSON")
}

// resetSubscribeFlags puts the subscribe command's flags back to their defaults,
// and registers the same for when the test ends.  Cobra keeps a flag's value after
// a command has run, which in a process that runs the command once is of no
// consequence but here would carry one test's flags into the next.
func resetSubscribeFlags(t *testing.T) {
	t.Helper()
	reset := func() {
		subscribeCmd.Flags().VisitAll(func(f *pflag.Flag) {
			_ = f.Value.Set(f.DefValue)
			f.Changed = false
		})
	}
	reset()
	t.Cleanup(reset)
}

// pushSource announces whatever the test pushes into it.
type pushSource struct {
	eventsource.DefaultNotifier
	events chan string
}

func (s *pushSource) Events() <-chan string { return s.events }

// capture collects what is written to a file the command prints to.
type capture struct {
	mu     sync.Mutex
	data   bytes.Buffer
	target **os.File
	file   *os.File
	writer *os.File
	done   chan struct{}
	once   sync.Once
}

func newCapture(t *testing.T, target **os.File) *capture {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	c := &capture{target: target, file: *target, writer: w, done: make(chan struct{})}
	*target = w
	go func() {
		defer close(c.done)
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			c.mu.Lock()
			c.data.Write(buf[:n])
			c.mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(c.finish)
	return c
}

// finish restores the file and waits until everything written to it has been
// collected, so that String is complete.
func (c *capture) finish() {
	c.once.Do(func() {
		*c.target = c.file
		c.writer.Close()
		<-c.done
	})
}

func (c *capture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data.String()
}

// End to end: the command asks a director, which names an origin's notification
// endpoint; it connects there, subscribes, and prints what the origin announces.
func TestSubscribeCommandListens(t *testing.T) {
	test_utils.InitClient(t, map[param.Param]any{param.TLSSkipVerify: true})
	resetSubscribeFlags(t)

	source := &pushSource{events: make(chan string)}
	notifications, err := notificationserver.New(notificationserver.NotificationServerArgs{EventSource: source})
	require.NoError(t, err)

	var server *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/pelican-configuration", func(w http.ResponseWriter, r *http.Request) {
		_, err := fmt.Fprintf(w, `{"director_endpoint": %q}`, server.URL)
		assert.NoError(t, err)
	})
	mux.Handle(server_structs.OriginNotifyRoute+"/", http.StripPrefix(server_structs.OriginNotifyRoute, notifications))
	mux.HandleFunc("/ns/", func(w http.ResponseWriter, r *http.Request) {
		notifyUrl := "wss" + strings.TrimPrefix(server.URL, "https") + server_structs.OriginNotifyRoute + "/"
		w.Header().Set("X-Pelican-Namespace", "namespace=/ns, require-token=false, notify-url="+notifyUrl)
		w.Header().Set("Location", server.URL+r.URL.Path)
		w.Header().Set("Link", fmt.Sprintf(`<%s%s>; rel="duplicate"; pri=1; depth=1`, server.URL, r.URL.Path))
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	server = httptest.NewTLSServer(mux)
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "https://")

	stdout := newCapture(t, &os.Stdout)
	stderr := newCapture(t, &os.Stderr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cobra gives a subcommand the root's context only if it has none, so one that
	// has run before would keep the earlier, cancelled, context.
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetContext(context.TODO())
		subscribeCmd.SetContext(context.TODO())
	})
	rootCmd.SetContext(ctx)
	subscribeCmd.SetContext(ctx)
	rootCmd.SetArgs([]string{"object", "subscribe", "-v", "--depth", "2", "--client-id", "smoke", "pelican://" + host + "/ns/dir"})
	done := make(chan error, 1)
	go func() { done <- rootCmd.Execute() }()

	// Events pushed before the origin has registered the subscription are dropped,
	// so keep announcing until one is printed.
	require.Eventually(t, func() bool {
		select {
		case source.events <- "/ns/dir/obj":
		case <-time.After(time.Second):
		}
		return strings.Contains(stdout.String(), "/ns/dir/obj\n")
	}, 20*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err, "interrupting the command is a clean stop")
	case <-time.After(20 * time.Second):
		t.Fatal("the command did not stop when interrupted")
	}

	stdout.finish()
	stderr.finish()

	// Standard output carries only the notifications.
	for _, line := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		assert.Equal(t, "/ns/dir/obj", line)
	}
	// Standard error carries the connection details, printed before listening.
	details := stderr.String()
	assert.Contains(t, details, "Endpoint:            wss://"+host+server_structs.OriginNotifyRoute)
	assert.Contains(t, details, "Client ID:           smoke")
	assert.Contains(t, details, "Depth:               2")
	assert.Contains(t, details, "Topic:               smoke/2/pelican/ns/dir")
	assert.Contains(t, details, "Subscription params: 2/pelican/ns/dir")
	assert.Equal(t, 1, strings.Count(details, "Endpoint:"), "details are printed once, before listening")
	assert.Contains(t, details, "Listening; press Ctrl-C to stop")
}

func TestWriteSubscription(t *testing.T) {
	result := client.SubscribeResult{
		Namespace:    "/ns",
		RequireToken: true,
		NotifyUrl:    "wss://origin.example.com:8444/api/v1.0/origin/notify",
		Origins:      []string{"https://origin.example.com:8443"},
		ClientID:     "client1",
		Depth:        2,
		Topic:        "client1/2/osdf/ns/dir",
	}

	var out bytes.Buffer
	require.NoError(t, writeSubscription(&out, result, false, true))
	assert.Contains(t, out.String(), "Topic:               client1/2/osdf/ns/dir")
	assert.NotContains(t, out.String(), "Listening", "nothing here is listening")

	out.Reset()
	require.NoError(t, writeSubscription(&out, result, true, true))
	var decoded client.SubscribeResult
	require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
	assert.Equal(t, result, decoded, "the JSON carries the whole result")
	assert.Equal(t, 1, strings.Count(out.String(), "\n"), "one JSON object on one line")
}

// subscribeFederation is a fake federation whose director names a notification
// endpoint, and which counts the connections made to that endpoint.
type subscribeFederation struct {
	server        *httptest.Server
	host          string
	notifications *pushSource
	connectsMu    sync.Mutex
	connects      int
}

func newSubscribeFederation(t *testing.T) *subscribeFederation {
	t.Helper()
	f := &subscribeFederation{notifications: &pushSource{events: make(chan string)}}
	handler, err := notificationserver.New(notificationserver.NotificationServerArgs{EventSource: f.notifications})
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/pelican-configuration", func(w http.ResponseWriter, r *http.Request) {
		_, err := fmt.Fprintf(w, `{"director_endpoint": %q}`, f.server.URL)
		assert.NoError(t, err)
	})
	mux.HandleFunc(server_structs.OriginNotifyRoute+"/", func(w http.ResponseWriter, r *http.Request) {
		f.connectsMu.Lock()
		f.connects++
		f.connectsMu.Unlock()
		http.StripPrefix(server_structs.OriginNotifyRoute, handler).ServeHTTP(w, r)
	})
	mux.HandleFunc("/ns/", func(w http.ResponseWriter, r *http.Request) {
		notifyUrl := "wss" + strings.TrimPrefix(f.server.URL, "https") + server_structs.OriginNotifyRoute + "/"
		w.Header().Set("X-Pelican-Namespace", "namespace=/ns, require-token=false, notify-url="+notifyUrl)
		w.Header().Set("Location", f.server.URL+r.URL.Path)
		w.Header().Set("Link", fmt.Sprintf(`<%s%s>; rel="duplicate"; pri=1; depth=1`, f.server.URL, r.URL.Path))
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	f.server = httptest.NewTLSServer(mux)
	t.Cleanup(f.server.Close)
	f.host = strings.TrimPrefix(f.server.URL, "https://")
	return f
}

func (f *subscribeFederation) connections() int {
	f.connectsMu.Lock()
	defer f.connectsMu.Unlock()
	return f.connects
}

// run executes `pelican object subscribe` with args, returning once it ends.
func runSubscribe(t *testing.T, args ...string) error {
	t.Helper()
	resetSubscribeFlags(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetContext(context.TODO())
		subscribeCmd.SetContext(context.TODO())
	})
	rootCmd.SetContext(ctx)
	subscribeCmd.SetContext(ctx)
	rootCmd.SetArgs(append([]string{"object", "subscribe"}, args...))
	return rootCmd.Execute()
}

func TestSubscribeCommandNoListen(t *testing.T) {
	test_utils.InitClient(t, map[param.Param]any{param.TLSSkipVerify: true})
	f := newSubscribeFederation(t)

	t.Run("text", func(t *testing.T) {
		stdout := newCapture(t, &os.Stdout)
		stderr := newCapture(t, &os.Stderr)
		require.NoError(t, runSubscribe(t, "--no-listen", "--depth", "3", "--client-id", "probe", "pelican://"+f.host+"/ns/dir"))
		stdout.finish()
		stderr.finish()

		// The details are the command's result, so they are on standard output, and
		// the command has returned rather than waiting for notifications.
		assert.Contains(t, stdout.String(), "Endpoint:            wss://"+f.host+server_structs.OriginNotifyRoute)
		assert.Contains(t, stdout.String(), "Topic:               probe/3/pelican/ns/dir")
		assert.Contains(t, stdout.String(), "Subscription params: 3/pelican/ns/dir")
		assert.NotContains(t, stderr.String(), "Listening")
		assert.NotContains(t, stdout.String(), "Listening")
	})

	t.Run("json", func(t *testing.T) {
		stdout := newCapture(t, &os.Stdout)
		require.NoError(t, runSubscribe(t, "--no-listen", "--json", "--client-id", "probe", "pelican://"+f.host+"/ns/dir"))
		stdout.finish()

		var result client.SubscribeResult
		require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &result), stdout.String())
		assert.Equal(t, "probe", result.ClientID)
		assert.Equal(t, 1, result.Depth)
		assert.Equal(t, "probe/1/pelican/ns/dir", result.Topic)
		assert.Equal(t, "wss://"+f.host+server_structs.OriginNotifyRoute+"/", result.NotifyUrl)
	})

	assert.Zero(t, f.connections(), "--no-listen never connects to the notification endpoint")
}
