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
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/clientqueue"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/eventsource"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/notificationserver"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/pelicanplatform/pelican/client"
	"github.com/pelicanplatform/pelican/origin_serve"
	"github.com/pelicanplatform/pelican/param"
	"github.com/pelicanplatform/pelican/server_structs"
	"github.com/pelicanplatform/pelican/server_utils"
	"github.com/pelicanplatform/pelican/token"
)

// notifyWalkInterval is how often the origin walks the paths its subscribers are
// watching, looking for objects to announce.
const notifyWalkInterval = time.Minute

// Lifetime of the token the notifier lists with, and how close to expiry it is
// replaced.
const (
	notifyTokenLifetime = 30 * time.Minute
	notifyTokenRefresh  = 5 * time.Minute
)

// exportSupportsNotify reports whether the origin announces changes to the
// export.  Notifications must be enabled; the export must have the Notify and
// Listings capabilities, since the notifier finds new objects by listing the
// export; and the export must be served by XRootD, whose HTTP endpoint the
// notifier lists.  Advertisement and the event source both use this, so the
// capability the director sees is exactly what the notifier will do.
func exportSupportsNotify(export server_utils.OriginExport) bool {
	if !param.Origin_EnableNotifications.GetBool() {
		return false
	}
	if !export.Capabilities.Notify || !export.Capabilities.Listings {
		return false
	}
	storageType, err := server_structs.ParseOriginStorageType(param.Origin_StorageType.GetString())
	return err == nil && storageType.UsesXRootD()
}

// watchRegion is a part of the namespace: an object path and how many levels of
// path below it count.  Objects directly beneath path are one level down, and
// path itself is level zero, which matters only if path names an object.
type watchRegion struct {
	path  string
	depth int
}

// levelsBelow reports how many path segments objPath is below base.  It is zero if
// they are the same path, and ok is false if objPath is not base or beneath it.
// It compares whole segments: /foo is not above /foobar.
func levelsBelow(base, objPath string) (levels int, ok bool) {
	if objPath == base {
		return 0, true
	}
	rest := objPath
	if base != "/" {
		if !strings.HasPrefix(objPath, base+"/") {
			return 0, false
		}
		rest = strings.TrimPrefix(objPath, base+"/")
	} else {
		rest = strings.TrimPrefix(objPath, "/")
	}
	if rest == "" {
		return 0, false
	}
	return strings.Count(rest, "/") + 1, true
}

// covers reports whether the region includes objPath.
func (r watchRegion) covers(objPath string) bool {
	levels, ok := levelsBelow(r.path, objPath)
	return ok && levels <= r.depth
}

// coversRegion reports whether walking r also walks everything in other.
func (r watchRegion) coversRegion(other watchRegion) bool {
	levels, ok := levelsBelow(r.path, other.path)
	return ok && r.depth >= other.depth+levels
}

// subscription is what a client has asked to hear about.
type subscription struct {
	watchRegion
	// since is when the subscription began.  Objects that already existed then
	// are not news to it.
	since time.Time
}

// parseSubscription reads a client's subscription parameters, the part of its
// topic after the client id (see server_structs.SubscriptionTopic): e.g.
// "2/osdf/vdc/public" asks for objects up to two levels below /vdc/public.
func parseSubscription(params string) (watchRegion, error) {
	depth, _, objPath, err := server_structs.ParseSubscriptionParams(params)
	if err != nil {
		return watchRegion{}, err
	}
	return watchRegion{path: objPath, depth: depth}, nil
}

// regionsFor returns the parts of the export at prefix that subs need walked: a
// subscription to a path inside the export needs that path walked to its depth,
// and one to a path above the export needs the export walked to whatever depth
// is left once the levels down to the export are spent.
func regionsFor(prefix string, subs []subscription) []watchRegion {
	var regions []watchRegion
	for _, sub := range subs {
		if _, inside := levelsBelow(prefix, sub.path); inside {
			regions = append(regions, sub.watchRegion)
		} else if levels, above := levelsBelow(sub.path, prefix); above {
			if remaining := sub.depth - levels; remaining >= 1 {
				regions = append(regions, watchRegion{path: prefix, depth: remaining})
			}
		}
	}
	return regions
}

// reduceRegions drops every region that another one in the list already walks,
// along with duplicates, and returns the rest ordered by path.
func reduceRegions(regions []watchRegion) []watchRegion {
	var reduced []watchRegion
	for i, r := range regions {
		redundant := false
		for j, other := range regions {
			if i == j || !other.coversRegion(r) {
				continue
			}
			// Of two regions that each cover the other, i.e. equal ones, keep the first.
			if !r.coversRegion(other) || j < i {
				redundant = true
				break
			}
		}
		if !redundant {
			reduced = append(reduced, r)
		}
	}
	sort.Slice(reduced, func(i, j int) bool { return reduced[i].path < reduced[j].path })
	return reduced
}

// objectWalker lists objects.
type objectWalker interface {
	// walk calls visit with the path and modification time of every object at
	// most depth levels below the namespace path root.  A root that does not
	// exist has no objects and is not an error.
	walk(ctx context.Context, root string, depth int, visit func(objPath string, mtime time.Time)) error
}

// propfindWalker walks the origin's namespace by listing it, with PROPFIND
// requests, at the origin's own XRootD endpoint.  XRootD exports each namespace at
// its federation prefix, so a namespace path is also the URL path.
type propfindWalker struct {
	lister *client.CollectionLister
}

func (w propfindWalker) walk(ctx context.Context, root string, depth int, visit func(objPath string, mtime time.Time)) error {
	err := w.lister.Walk(ctx, root, depth,
		func(fi client.FileInfo) { visit(fi.Name, fi.ModTime) },
		func(dir string, err error) { log.Warnf("Object notifier skipping %s: %v", dir, err) })
	if errors.Is(err, client.ErrCollectionNotFound) {
		log.Debugf("Object notifier: %s does not exist", root)
		return nil
	}
	return err
}

// selfIssuedToken is a client.TokenProvider that issues the origin's own token
// for reading everything it serves, and replaces it before it expires.
type selfIssuedToken struct {
	mu      sync.Mutex
	token   string
	expires time.Time
}

func (s *selfIssuedToken) Get() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && time.Until(s.expires) > notifyTokenRefresh {
		return s.token, nil
	}

	tokenConfig := token.NewWLCGToken()
	tokenConfig.Lifetime = notifyTokenLifetime
	tokenConfig.Subject = "origin-notifier"
	tokenConfig.AddAudienceAny()
	tokenConfig.AddRawScope("storage.read:/")
	tok, err := tokenConfig.CreateToken()
	if err != nil {
		return "", errors.Wrap(err, "failed to create the notifier's token")
	}
	s.token, s.expires = tok, time.Now().Add(notifyTokenLifetime)
	return s.token, nil
}

// walkedRegion is a region the previous walk covered, and the modification time of
// the newest object it found there.
type walkedRegion struct {
	watchRegion
	newest time.Time
}

// objectEventSource is the eventsource.EventSource the origin gives the
// notification server.  Once per interval it walks the parts of the namespace its
// subscribers are watching -- and nothing else, so with no subscribers it walks
// nothing -- and emits the path of each object that is new or has changed since
// the last walk of that part.  Deletions are not announced.
//
// An object is new if its modification time is newer than the newest one seen in
// the previous walk of the region it lies in.  Where the previous walk did not
// reach, because the subscription is new or asks for more depth than before, it is
// new only if it is newer than the moment the subscription began.  An object that
// is written with an older modification time than that is therefore not announced,
// nor is one that shares it to the last representable tick.
//
// Nothing is kept across restarts, as the subscriptions are not either.
type objectEventSource struct {
	eventsource.DefaultNotifier

	events   chan string
	exports  func() ([]server_utils.OriginExport, error)
	walker   objectWalker
	interval time.Duration
	now      func() time.Time

	mu   sync.Mutex
	subs map[clientqueue.Queue]subscription

	// previous belongs to the goroutine in run (and to walkOnce when a test calls
	// it directly).
	previous []walkedRegion
}

func newObjectEventSource(walker objectWalker) *objectEventSource {
	return &objectEventSource{
		events:   make(chan string),
		exports:  server_utils.GetOriginExports,
		walker:   walker,
		interval: notifyWalkInterval,
		now:      time.Now,
		subs:     map[clientqueue.Queue]subscription{},
	}
}

// Events implements eventsource.EventSource.
func (s *objectEventSource) Events() <-chan string { return s.events }

// QueueAdded implements eventsource.EventSource: a client has subscribed, so what
// it is watching is now walked.
func (s *objectEventSource) QueueAdded(q clientqueue.Queue) {
	params := q.Params()
	region, err := parseSubscription(params)
	if err != nil {
		log.Warnf("Object notifier ignoring a subscription it cannot read: %v", err)
		return
	}
	log.Debugf("Object notifier: subscriber with parameters %q now watches %s to depth %d", params, region.path, region.depth)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[q] = subscription{watchRegion: region, since: s.now()}
}

// QueueRemoved implements eventsource.EventSource: a client has unsubscribed.
func (s *objectEventSource) QueueRemoved(q clientqueue.Queue) {
	log.Debugf("Object notifier: subscriber with parameters %q has unsubscribed", q.Params())
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, q)
}

// ShouldNotify implements eventsource.EventSource: a client hears about an event
// only if the object lies within the path and depth it subscribed to.
func (s *objectEventSource) ShouldNotify(event string, q clientqueue.Queue) bool {
	params := q.Params()
	region, err := parseSubscription(params)
	notify := err == nil && region.covers(event)
	log.Debugf("Object notifier: new object %q, subscriber with parameters %q: notify=%t", event, params, notify)
	return notify
}

func (s *objectEventSource) subscriptions() []subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	subs := make([]subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, sub)
	}
	return subs
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

// announceThreshold returns the modification time an object must be newer than to
// be announced, and whether anyone is watching it at all.
func (s *objectEventSource) announceThreshold(objPath string, subs []subscription) (threshold time.Time, watched bool) {
	var earliestSince time.Time
	for _, sub := range subs {
		if sub.covers(objPath) {
			if !watched || sub.since.Before(earliestSince) {
				earliestSince = sub.since
			}
			watched = true
		}
	}
	if !watched {
		return time.Time{}, false
	}

	// Where the previous walk reached, the object is new if it is newer than
	// what that walk saw.  If more than one such region reached it, trust the
	// least of them: better to repeat an announcement than to miss one.
	var previous time.Time
	walkedBefore := false
	for _, region := range s.previous {
		if region.covers(objPath) && (!walkedBefore || region.newest.Before(previous)) {
			previous, walkedBefore = region.newest, true
		}
	}
	if walkedBefore {
		return previous, true
	}
	return earliestSince, true
}

// walkOnce walks the parts of the namespace the subscribers are watching and emits
// the path of each object that is new since the last walk.
func (s *objectEventSource) walkOnce(ctx context.Context) error {
	subs := s.subscriptions()
	if len(subs) == 0 {
		log.Debugf("Object notifier: no subscribers, so no walk")
		s.previous = nil
		return nil
	}
	log.Debugf("Object notifier: starting walk for %d subscriber(s)", len(subs))

	exports, err := s.exports()
	if err != nil {
		return errors.Wrap(err, "failed to get origin exports")
	}
	var regions []watchRegion
	for _, export := range exports {
		if exportSupportsNotify(export) {
			regions = append(regions, regionsFor(export.FederationPrefix, subs)...)
		}
	}
	regions = reduceRegions(regions)

	type change struct {
		path  string
		mtime time.Time
	}
	var changes []change
	seen := map[string]struct{}{}
	var walked []walkedRegion

	for _, region := range regions {
		var newest time.Time
		err := s.walker.walk(ctx, region.path, region.depth, func(objPath string, mtime time.Time) {
			if mtime.After(newest) {
				newest = mtime
			}
			if _, dup := seen[objPath]; dup {
				return
			}
			seen[objPath] = struct{}{}
			if threshold, watched := s.announceThreshold(objPath, subs); watched && mtime.After(threshold) {
				log.Debugf("Object notifier: found new or modified object %q (mtime %s)", objPath, mtime.Format(time.RFC3339Nano))
				changes = append(changes, change{objPath, mtime})
			}
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Leave the region out of what this walk reached, so that the next
			// one treats it as new rather than trusting a walk that did not finish.
			log.Warnf("Object notifier failed to walk %s: %v", region.path, err)
			continue
		}
		walked = append(walked, walkedRegion{watchRegion: region, newest: newest})
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
	log.Debugf("Object notifier: walk finished; %d new or modified object(s) announced", len(changes))

	s.previous = walked
	return nil
}

// notifyCredentials is the Principal.Data of a connection: the verified tokens
// its upgrade request carried, which may be none.
type notifyCredentials []string

// notifyAuthenticator admits a connection if every token it presents is valid.
// A connection with no token is admitted anonymously, and can then subscribe
// only to what is publicly readable.
type notifyAuthenticator struct{}

func (notifyAuthenticator) Authenticate(w http.ResponseWriter, r *http.Request) (notificationserver.Principal, bool) {
	tokens := origin_serve.ExtractTokens(r)
	if len(tokens) == 0 {
		return notificationserver.Principal{}, true
	}
	user, valid := origin_serve.VerifyTokens(tokens)
	if len(valid) == 0 {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "the token presented is not valid for this origin", http.StatusUnauthorized)
		return notificationserver.Principal{}, false
	}
	if user == "" {
		user = "authenticated"
	}
	return notificationserver.Principal{Name: user, Data: notifyCredentials(valid)}, true
}

// notifyAuthorizer lets a connection subscribe to a topic only if it could read
// the topic's object path from the origin.
type notifyAuthorizer struct{}

func (notifyAuthorizer) Authorize(p notificationserver.Principal, destination string) bool {
	// The destination is the topic, which starts with the client id.
	_, params, found := strings.Cut(strings.TrimPrefix(destination, "/"), "/")
	if !found {
		return false
	}
	_, _, objPath, err := server_structs.ParseSubscriptionParams(params)
	if err != nil {
		return false
	}
	tokens, _ := p.Data.(notifyCredentials)
	return origin_serve.AuthorizeRead(objPath, tokens)
}

// mountObjectNotifier serves the notification server fed by source on engine at
// server_structs.OriginNotifyRoute.
func mountObjectNotifier(engine *gin.Engine, source eventsource.EventSource) error {
	handler, err := notificationserver.New(notificationserver.NotificationServerArgs{
		EventSource:   source,
		Authenticator: notifyAuthenticator{},
		Authorizer:    notifyAuthorizer{},
	})
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
// its websocket endpoint on engine.  It lists the origin's own XRootD endpoint,
// so it must run once XRootD is serving.
func LaunchObjectNotifier(ctx context.Context, egrp *errgroup.Group, engine *gin.Engine) error {
	originUrl, err := url.Parse(param.Origin_Url.GetString())
	if err != nil || originUrl.Host == "" {
		return errors.Errorf("cannot list the origin at %s=%q: it is not a URL with a host", param.Origin_Url.GetName(), param.Origin_Url.GetString())
	}
	source := newObjectEventSource(propfindWalker{lister: client.NewCollectionLister(originUrl, &selfIssuedToken{})})
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
