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
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/clientqueue"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/eventsource"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/notificationserver"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/webdav"
	"golang.org/x/sync/errgroup"

	"github.com/pelicanplatform/pelican/origin_serve"
	"github.com/pelicanplatform/pelican/param"
	"github.com/pelicanplatform/pelican/server_structs"
	"github.com/pelicanplatform/pelican/server_utils"
)

// notifyWalkInterval is how often the origin walks its exports looking for
// objects to announce.
const notifyWalkInterval = time.Minute

// exportSupportsNotify reports whether the origin can announce changes to the
// export.  The export must have the Notify capability, and the process must be
// serving it from a backend whose filesystem it can walk; an XRootD-fronted
// export has no such backend.  Advertisement and the event source both use this,
// so the capability the director sees is exactly what the walker will do.
func exportSupportsNotify(export server_utils.OriginExport) bool {
	if !export.Capabilities.Notify {
		return false
	}
	backend := origin_serve.BackendForPrefix(export.FederationPrefix)
	return backend != nil && backend.FileSystem() != nil
}

// mtimeState persists the modification time of the newest object the origin has
// announced, so that a restart does not announce it all again.
type mtimeState struct {
	file string
}

// load returns the stored modification time.  ok is false if nothing has been
// stored yet.
func (s mtimeState) load() (t time.Time, ok bool, err error) {
	data, err := os.ReadFile(s.file)
	if os.IsNotExist(err) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	t, err = time.Parse(time.RFC3339Nano, strings.TrimSpace(string(data)))
	if err != nil {
		return time.Time{}, false, errors.Wrapf(err, "malformed notifier state in %s", s.file)
	}
	return t, true, nil
}

// store writes t atomically, so a crash never leaves a truncated state file.
func (s mtimeState) store(t time.Time) error {
	if err := os.MkdirAll(filepath.Dir(s.file), 0o750); err != nil {
		return err
	}
	tmp := s.file + ".tmp"
	if err := os.WriteFile(tmp, []byte(t.UTC().Format(time.RFC3339Nano)+"\n"), 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, s.file)
}

// objectEventSource is the eventsource.EventSource the origin gives the
// notification server.  Once per interval it walks every notifiable export and
// emits the federation path of each object whose modification time is newer than
// the newest one it has already announced.  Deletions are not announced.
//
// Only that newest modification time is kept.  An object written with an older
// modification time than the stored one (for instance a copy that preserves
// mtimes) is therefore not announced, nor is one that shares the stored time to
// the last representable tick.
type objectEventSource struct {
	eventsource.DefaultNotifier

	events   chan string
	exports  func() ([]server_utils.OriginExport, error)
	fsFor    func(export server_utils.OriginExport) webdav.FileSystem
	state    mtimeState
	interval time.Duration

	// lastMtime and seeded belong to the goroutine in run (and to walkOnce when
	// a test calls it directly).
	lastMtime time.Time
	seeded    bool
}

func newObjectEventSource(stateFile string) *objectEventSource {
	return &objectEventSource{
		events:  make(chan string),
		exports: server_utils.GetOriginExports,
		fsFor: func(export server_utils.OriginExport) webdav.FileSystem {
			if !exportSupportsNotify(export) {
				return nil
			}
			return origin_serve.BackendForPrefix(export.FederationPrefix).FileSystem()
		},
		state:    mtimeState{file: stateFile},
		interval: notifyWalkInterval,
	}
}

// Events implements eventsource.EventSource.
func (s *objectEventSource) Events() <-chan string { return s.events }

// ShouldNotify implements eventsource.EventSource: a client hears about an event
// only if the object lies under the path it subscribed to.
func (s *objectEventSource) ShouldNotify(event string, q clientqueue.Queue) bool {
	subscribed, ok := subscribedPath(q.Params())
	return ok && pathCovers(subscribed, event)
}

// subscribedPath extracts the object path from a client's subscription
// parameters, which have the form "<protocol>/<object path>", e.g.
// "osdf/vdc/public" names /vdc/public.  ok is false if params has no protocol
// component.
func subscribedPath(params string) (subscribed string, ok bool) {
	_, rest, found := strings.Cut(params, "/")
	if !found {
		return "", false
	}
	return path.Clean("/" + rest), true
}

// pathCovers reports whether objPath is subscribed or lies beneath it, comparing
// whole path segments: /foo covers /foo and /foo/bar, but not /foobar.
func pathCovers(subscribed, objPath string) bool {
	if subscribed == "/" {
		return true
	}
	return objPath == subscribed || strings.HasPrefix(objPath, subscribed+"/")
}

// run walks once immediately and then every interval until ctx is done, at which
// point it closes the events channel.
func (s *objectEventSource) run(ctx context.Context) {
	defer close(s.events)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		if err := s.walkOnce(ctx); err != nil && ctx.Err() == nil {
			log.Warningf("Object notifier walk failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// walkOnce walks every notifiable export and emits an event for each object newer
// than the stored modification time.  The very first walk on an origin with no
// stored state only records the newest modification time it sees: announcing every
// object already in the store would be a flood of non-news.
func (s *objectEventSource) walkOnce(ctx context.Context) error {
	if !s.seeded {
		last, ok, err := s.state.load()
		if err != nil {
			return errors.Wrap(err, "failed to load notifier state")
		}
		s.lastMtime, s.seeded = last, true
		if !ok {
			s.lastMtime = time.Time{}
		}
	}
	baseline := s.lastMtime.IsZero()

	exports, err := s.exports()
	if err != nil {
		return errors.Wrap(err, "failed to get origin exports")
	}

	type change struct {
		path  string
		mtime time.Time
	}
	var changes []change
	newest := s.lastMtime
	walkStart := time.Now()

	for _, export := range exports {
		fs := s.fsFor(export)
		if fs == nil {
			continue
		}
		err := walkFiles(ctx, fs, "/", func(relPath string, mtime time.Time) {
			if mtime.After(newest) {
				newest = mtime
			}
			if !baseline && mtime.After(s.lastMtime) {
				changes = append(changes, change{path.Join(export.FederationPrefix, relPath), mtime})
			}
		})
		if err != nil {
			return errors.Wrapf(err, "failed to walk export %s", export.FederationPrefix)
		}
	}

	// Announce oldest first, so subscribers see changes in the order they happened.
	sort.SliceStable(changes, func(i, j int) bool { return changes[i].mtime.Before(changes[j].mtime) })
	for _, c := range changes {
		select {
		case s.events <- c.path:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if baseline && newest.IsZero() {
		// An empty store: anything written from now on is newer than this.
		newest = walkStart
	}
	if newest.After(s.lastMtime) {
		if err := s.state.store(newest); err != nil {
			return errors.Wrap(err, "failed to store notifier state")
		}
		s.lastMtime = newest
	}
	return nil
}

// walkFiles calls visit with the path (relative to the filesystem root) and
// modification time of every non-directory beneath dir.  A subtree that cannot be
// read is logged and skipped, so one unreadable directory does not blind the
// walker to the rest of the store.
func walkFiles(ctx context.Context, fs webdav.FileSystem, dir string, visit func(relPath string, mtime time.Time)) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	f, err := fs.OpenFile(ctx, dir, os.O_RDONLY, 0)
	if err != nil {
		return errors.Wrapf(err, "failed to open %s", dir)
	}
	entries, err := f.Readdir(-1)
	if closeErr := f.Close(); closeErr != nil {
		log.Debugf("Error closing %s after listing: %v", dir, closeErr)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to list %s", dir)
	}

	for _, entry := range entries {
		child := path.Join(dir, entry.Name())
		if !entry.IsDir() {
			visit(child, entry.ModTime())
			continue
		}
		if err := walkFiles(ctx, fs, child, visit); err != nil {
			if ctx.Err() != nil {
				return err
			}
			log.Warnf("Object notifier skipping %s: %v", child, err)
		}
	}
	return nil
}

// mountObjectNotifier serves the notification server fed by source on engine at
// server_structs.OriginNotifyRoute.
func mountObjectNotifier(engine *gin.Engine, source eventsource.EventSource) error {
	handler, err := notificationserver.New(notificationserver.NotificationServerArgs{EventSource: source})
	if err != nil {
		return errors.Wrap(err, "failed to create the notification server")
	}

	route := server_structs.OriginNotifyRoute
	stripped := http.StripPrefix(route, handler)
	serve := func(c *gin.Context) {
		r := c.Request
		if r.URL.Path == route {
			// The notification server serves its websocket at "/"; keep a bare
			// route from being redirected there.
			r2 := r.Clone(r.Context())
			r2.URL.Path = route + "/"
			r = r2
		}
		stripped.ServeHTTP(c.Writer, r)
	}
	engine.Any(route, serve)
	engine.Any(route+"/*path", serve)
	return nil
}

// LaunchObjectNotifier starts the origin's object-notification service and mounts
// its websocket endpoint on engine.  It must run after the origin's storage
// backends have been registered.
func LaunchObjectNotifier(ctx context.Context, egrp *errgroup.Group, engine *gin.Engine) error {
	source := newObjectEventSource(param.Origin_NotifierStateLocation.GetString())
	if err := mountObjectNotifier(engine, source); err != nil {
		return err
	}

	egrp.Go(func() error {
		source.run(ctx)
		return nil
	})
	log.Infof("Object notifier serving at %s", server_structs.OriginNotifyRoute)
	return nil
}
