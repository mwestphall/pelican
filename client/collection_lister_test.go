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
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/webdav"

	"github.com/pelicanplatform/pelican/param"
	"github.com/pelicanplatform/pelican/test_utils"
)

// listerFixture serves a temporary directory over WebDAV, standing in for an
// XRootD origin's HTTP endpoint, and records the Authorization header and Depth
// header of every PROPFIND it answers.
type listerFixture struct {
	t      *testing.T
	root   string
	lister *CollectionLister

	mu      sync.Mutex
	auths   []string
	depths  []string
	propfds int
}

type staticToken string

func (s staticToken) Get() (string, error) { return string(s), nil }

var listerEpoch = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newListerFixture(t *testing.T, tokens TokenProvider) *listerFixture {
	t.Helper()
	test_utils.InitClient(t, map[param.Param]any{})
	f := &listerFixture{t: t, root: t.TempDir()}
	dav := &webdav.Handler{FileSystem: webdav.Dir(f.root), LockSystem: webdav.NewMemLS()}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			f.mu.Lock()
			f.propfds++
			f.auths = append(f.auths, r.Header.Get("Authorization"))
			f.depths = append(f.depths, r.Header.Get("Depth"))
			f.mu.Unlock()
		}
		dav.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	serverUrl, err := url.Parse(server.URL)
	require.NoError(t, err)
	f.lister = NewCollectionLister(serverUrl, tokens)
	return f
}

func (f *listerFixture) write(rel string, age time.Duration) {
	f.t.Helper()
	full := filepath.Join(f.root, rel)
	require.NoError(f.t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(f.t, os.WriteFile(full, []byte("x"), 0o644))
	mtime := listerEpoch.Add(age)
	require.NoError(f.t, os.Chtimes(full, mtime, mtime))
}

func (f *listerFixture) walk(root string, depth int) (visited map[string]time.Time, skipped map[string]error, err error) {
	f.t.Helper()
	visited, skipped = map[string]time.Time{}, map[string]error{}
	err = f.lister.Walk(context.Background(), root, depth,
		func(fi FileInfo) {
			assert.False(f.t, fi.IsCollection, "only objects are visited")
			visited[fi.Name] = fi.ModTime
		},
		func(p string, err error) { skipped[p] = err })
	return
}

func names(m map[string]time.Time) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestCollectionListerDepth(t *testing.T) {
	f := newListerFixture(t, nil)
	f.write("ns/top", time.Minute)
	f.write("ns/a/mid", 2*time.Minute)
	f.write("ns/a/b/deep", 3*time.Minute)
	f.write("ns/a/b/c/deeper", 4*time.Minute)

	testCases := []struct {
		depth    int
		expected []string
	}{
		{1, []string{"/ns/top"}},
		{2, []string{"/ns/a/mid", "/ns/top"}},
		{3, []string{"/ns/a/b/deep", "/ns/a/mid", "/ns/top"}},
		{4, []string{"/ns/a/b/c/deeper", "/ns/a/b/deep", "/ns/a/mid", "/ns/top"}},
		{99, []string{"/ns/a/b/c/deeper", "/ns/a/b/deep", "/ns/a/mid", "/ns/top"}},
	}
	for _, tc := range testCases {
		visited, skipped, err := f.walk("/ns", tc.depth)
		require.NoError(t, err, "depth %d", tc.depth)
		assert.Empty(t, skipped)
		assert.Equal(t, tc.expected, names(visited), "depth %d", tc.depth)
	}

	// Modification times come back with the listing.
	visited, _, err := f.walk("/ns", 2)
	require.NoError(t, err)
	assert.True(t, visited["/ns/a/mid"].Equal(listerEpoch.Add(2*time.Minute)), "got %v", visited["/ns/a/mid"])
}

func TestCollectionListerUsesOnePropfindPerCollection(t *testing.T) {
	f := newListerFixture(t, nil)
	f.write("ns/top", time.Minute)
	f.write("ns/a/mid", time.Minute)
	f.write("ns/a/b/deep", time.Minute)

	_, _, err := f.walk("/ns", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, f.propfds, "one for /ns, /ns/a and /ns/a/b")
	for _, depth := range f.depths {
		assert.Equal(t, "1", depth, "a listing never asks for more than one level at a time")
	}

	// A depth limit stops the descent, so the deeper collection is never listed.
	f.propfds = 0
	_, _, err = f.walk("/ns", 2)
	require.NoError(t, err)
	assert.Equal(t, 2, f.propfds, "/ns/a/b is beyond the depth limit")
}

func TestCollectionListerNames(t *testing.T) {
	f := newListerFixture(t, nil)
	f.write("ns/with space/file name.txt", time.Minute)
	f.write("ns/percent%20/100% done", time.Minute)

	visited, skipped, err := f.walk("/ns", 3)
	require.NoError(t, err)
	assert.Empty(t, skipped)
	assert.Equal(t, []string{"/ns/percent%20/100% done", "/ns/with space/file name.txt"}, names(visited),
		"names are returned unescaped, and directories with unusual names are still descended into")
}

func TestCollectionListerRootIsAnObject(t *testing.T) {
	f := newListerFixture(t, nil)
	f.write("ns/object", 5*time.Minute)

	visited, _, err := f.walk("/ns/object", 1)
	require.NoError(t, err)
	assert.Equal(t, []string{"/ns/object"}, names(visited))
	assert.True(t, visited["/ns/object"].Equal(listerEpoch.Add(5*time.Minute)))
}

func TestCollectionListerMissingRoot(t *testing.T) {
	f := newListerFixture(t, nil)
	_, _, err := f.walk("/nope", 1)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCollectionNotFound)
}

func TestCollectionListerRejectsBadDepth(t *testing.T) {
	f := newListerFixture(t, nil)
	for _, depth := range []int{0, -1} {
		_, _, err := f.walk("/", depth)
		require.Error(t, err, "depth %d", depth)
		assert.Zero(t, f.propfds, "nothing is requested for an invalid depth")
	}
}

func TestCollectionListerSendsTheCurrentToken(t *testing.T) {
	f := newListerFixture(t, staticToken("secret"))
	f.write("ns/a/obj", time.Minute)
	_, _, err := f.walk("/ns", 2)
	require.NoError(t, err)
	require.NotEmpty(t, f.auths)
	for _, auth := range f.auths {
		assert.Equal(t, "Bearer secret", auth)
	}

	anonymous := newListerFixture(t, nil)
	anonymous.write("ns/obj", time.Minute)
	_, _, err = anonymous.walk("/ns", 1)
	require.NoError(t, err)
	assert.Equal(t, []string{""}, anonymous.auths, "no provider, no Authorization header")
}

func TestCollectionListerStopsWhenCancelled(t *testing.T) {
	f := newListerFixture(t, nil)
	f.write("ns/a/obj", time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := f.lister.Walk(ctx, "/ns", 2, func(FileInfo) {}, nil)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, f.propfds)
}
