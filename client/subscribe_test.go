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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pelicanplatform/pelican/param"
	"github.com/pelicanplatform/pelican/pelican_url"
	"github.com/pelicanplatform/pelican/server_structs"
	"github.com/pelicanplatform/pelican/test_utils"
)

// subscribeDirector is a fake federation: it serves discovery metadata and,
// like a director, answers requests for an object path with a redirect.
type subscribeDirector struct {
	server *httptest.Server

	mu           sync.Mutex
	queries      []string // raw query of every request for an object path
	withNotify   bool     // whether the response carries a notify-url
	failStatus   int      // if non-zero, answer object requests with this status
	originNotify string
}

func newSubscribeDirector(t *testing.T) *subscribeDirector {
	t.Helper()
	test_utils.InitClient(t, map[param.Param]any{param.TLSSkipVerify: true})
	d := &subscribeDirector{withNotify: true, originNotify: "wss://origin.example.com:8444" + server_structs.OriginNotifyRoute}
	d.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pelican_url.PelicanDiscoveryPath {
			_, err := fmt.Fprintf(w, `{"director_endpoint": %q}`, d.server.URL)
			assert.NoError(t, err)
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		d.queries = append(d.queries, r.URL.RawQuery)
		if d.failStatus != 0 {
			w.WriteHeader(d.failStatus)
			return
		}
		ns := "namespace=/ns, require-token=false"
		if d.withNotify && r.URL.Query().Has(pelican_url.QueryNotify) {
			ns += ", notify-url=" + d.originNotify
		}
		w.Header().Set("X-Pelican-Namespace", ns)
		w.Header().Set("Location", "https://origin.example.com:8443"+r.URL.Path)
		w.Header().Set("Link", fmt.Sprintf(`<https://origin.example.com:8443%s>; rel="duplicate"; pri=1; depth=1, `+
			`<https://origin2.example.com:8443%s>; rel="duplicate"; pri=2; depth=1`, r.URL.Path, r.URL.Path))
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(d.server.Close)
	return d
}

// url returns a pelican:// URL for path, with the fake federation as its host.
func (d *subscribeDirector) url(t *testing.T, path string) string {
	t.Helper()
	u, err := url.Parse(d.server.URL)
	require.NoError(t, err)
	return "pelican://" + u.Host + path
}

func (d *subscribeDirector) requests() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.queries)
}

func TestDoSubscribe(t *testing.T) {
	d := newSubscribeDirector(t)

	result, err := DoSubscribe(context.Background(), d.url(t, "/ns/dir"), 2, "client1")
	require.NoError(t, err)

	assert.Equal(t, "/ns", result.Namespace)
	assert.False(t, result.RequireToken)
	assert.Equal(t, "wss://origin.example.com:8444"+server_structs.OriginNotifyRoute, result.NotifyUrl)
	assert.Equal(t, []string{"https://origin.example.com:8443", "https://origin2.example.com:8443"}, result.Origins)
	assert.Equal(t, "client1", result.ClientID)
	assert.Equal(t, 2, result.Depth)
	assert.Equal(t, "client1/2/pelican/ns/dir", result.Topic, "<client id>/<depth>/<protocol>/<path>")

	require.Equal(t, 1, d.requests())
	assert.Contains(t, d.queries[0], pelican_url.QueryNotify, "the director is asked about notifications")
}

func TestDoSubscribeTopicRoundTripsThroughTheOriginsParser(t *testing.T) {
	d := newSubscribeDirector(t)

	result, err := DoSubscribe(context.Background(), d.url(t, "/ns/dir/"), 3, "my-client")
	require.NoError(t, err)

	// The notification server hands the origin everything after the client id.
	clientID, params, found := strings.Cut(result.Topic, "/")
	require.True(t, found)
	assert.Equal(t, "my-client", clientID)
	depth, protocol, objPath, err := server_structs.ParseSubscriptionParams(params)
	require.NoError(t, err)
	assert.Equal(t, 3, depth)
	assert.Equal(t, "pelican", protocol)
	assert.Equal(t, "/ns/dir", objPath)
}

func TestDoSubscribeMakesAClientIDWhenNoneIsGiven(t *testing.T) {
	d := newSubscribeDirector(t)

	first, err := DoSubscribe(context.Background(), d.url(t, "/ns"), 1, "")
	require.NoError(t, err)
	second, err := DoSubscribe(context.Background(), d.url(t, "/ns"), 1, "")
	require.NoError(t, err)

	assert.NotEmpty(t, first.ClientID)
	assert.NotContains(t, first.ClientID, "/")
	assert.NotEqual(t, first.ClientID, second.ClientID, "a subscription's id is unique unless chosen")
	assert.Equal(t, first.ClientID+"/1/pelican/ns", first.Topic)
}

func TestDoSubscribeRejectsBadArgumentsBeforeAskingTheDirector(t *testing.T) {
	d := newSubscribeDirector(t)

	for _, depth := range []int{0, -3} {
		_, err := DoSubscribe(context.Background(), d.url(t, "/ns"), depth, "c")
		require.Error(t, err, "depth %d", depth)
		assert.Contains(t, err.Error(), "depth")
	}
	_, err := DoSubscribe(context.Background(), d.url(t, "/ns"), 1, "has/slash")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client id")
	assert.Zero(t, d.requests(), "nothing was asked of the director")
}

func TestDoSubscribeWhenNoOriginOffersNotifications(t *testing.T) {
	d := newSubscribeDirector(t)
	d.withNotify = false
	_, err := DoSubscribe(context.Background(), d.url(t, "/ns"), 1, "c")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "notification endpoint")

	d.failStatus = http.StatusMethodNotAllowed
	_, err = DoSubscribe(context.Background(), d.url(t, "/ns"), 1, "c")
	assert.Error(t, err, "a director that refuses the request is an error")
}

func TestSubscriptionTopicForUsesTheUrlScheme(t *testing.T) {
	test_utils.InitClient(t, map[param.Param]any{})

	testCases := []struct {
		rawUrl   string
		expected string
	}{
		{"pelican://fed.example.com/ns/dir", "c/2/pelican/ns/dir"},
		{"pelican://fed.example.com/ns/dir?recursive", "c/2/pelican/ns/dir"},
		{"pelican://fed.example.com/", "c/2/pelican/"},
		{"osdf:///ns/dir", "c/2/osdf/ns/dir"},
	}
	for _, tc := range testCases {
		pUrl, err := pelican_url.Parse(tc.rawUrl, nil, nil)
		require.NoError(t, err, tc.rawUrl)
		topic, err := subscriptionTopicFor(pUrl, "c", 2)
		require.NoError(t, err, tc.rawUrl)
		assert.Equal(t, tc.expected, topic, tc.rawUrl)
	}
}

// The credential is found as for other commands: named, or else discovered, and
// a subscription to a public namespace goes without one if there is none.
func TestDoSubscribeToken(t *testing.T) {
	d := newSubscribeDirector(t)
	// Keep the developer's own credentials out of it.
	for _, env := range []string{"BEARER_TOKEN", "BEARER_TOKEN_FILE", "XDG_RUNTIME_DIR", "_CONDOR_CREDS", "TOKEN"} {
		t.Setenv(env, "")
	}
	t.Setenv("HOME", t.TempDir())
	subscribe := func(options ...TransferOption) (SubscribeResult, error) {
		return DoSubscribe(context.Background(), d.url(t, "/ns/dir"), 1, "c", options...)
	}

	t.Run("NoneFoundIsAnonymous", func(t *testing.T) {
		result, err := subscribe()
		require.NoError(t, err)
		assert.Empty(t, result.token)
	})
	t.Run("TokenGiven", func(t *testing.T) {
		result, err := subscribe(WithToken("given-token"))
		require.NoError(t, err)
		assert.Equal(t, "given-token", result.token)
	})
	t.Run("TokenFile", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(file, []byte("file-token\n"), 0600))
		result, err := subscribe(WithTokenLocation(file))
		require.NoError(t, err)
		assert.Equal(t, "file-token", result.token)
	})
	t.Run("NamedFileThatIsMissingIsAnError", func(t *testing.T) {
		_, err := subscribe(WithTokenLocation(filepath.Join(t.TempDir(), "absent")))
		require.Error(t, err)
	})
	t.Run("EmptyLocationIsNotNaming", func(t *testing.T) {
		result, err := subscribe(WithTokenLocation(""))
		require.NoError(t, err)
		assert.Empty(t, result.token)
	})
	t.Run("TokenIsNotInTheJSON", func(t *testing.T) {
		result, err := subscribe(WithToken("secret-token"))
		require.NoError(t, err)
		data, err := json.Marshal(result)
		require.NoError(t, err)
		assert.NotContains(t, string(data), "secret-token")
	})
}
