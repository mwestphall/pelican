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
	"io/fs"
	"net/url"
	"path"

	"github.com/pkg/errors"
	"github.com/studio-b12/gowebdav"

	"github.com/pelicanplatform/pelican/config"
)

// ErrCollectionNotFound is returned by CollectionLister.Walk when the collection
// it was asked to walk does not exist.
var ErrCollectionNotFound = errors.New("collection not found")

// CollectionLister lists the objects of a server's collections with WebDAV
// PROPFIND requests, starting from a collections URL it is given.
//
// It asks no director anything.  The listing behind `pelican object ls` first
// resolves the path through the federation; a caller that already knows which
// server to ask -- an origin listing its own namespace, say -- can use this
// instead.
type CollectionLister struct {
	webdav *gowebdav.Client
}

// NewCollectionLister returns a lister for the server at collectionsUrl.
// Every request carries a bearer token from tokens, which is asked for a token
// each time and so may refresh it; a nil tokens sends requests without one.
func NewCollectionLister(collectionsUrl *url.URL, tokens TokenProvider) *CollectionLister {
	var generator *tokenGenerator
	if tokens != nil {
		generator = newTokenGenerator(nil, nil, config.TokenRead, false)
		generator.SetExternalProvider(tokens)
	}
	return &CollectionLister{webdav: createWebDavClient(collectionsUrl, generator, "")}
}

// Walk calls visit for every object -- not collection -- at most depth levels
// below root, using one PROPFIND per collection.  Objects directly in root are
// one level down, those in its subcollections two, and so on, so depth must be at
// least 1.  If root is itself an object, visit is called for it alone.
//
// Names passed to visit are root joined with the entry's path, unescaped.
//
// A subcollection that cannot be listed is reported to onError (which may be
// nil) and skipped, so one unreadable directory does not hide the rest.  A
// failure to list root itself is returned, as ErrCollectionNotFound if root does
// not exist.  Cancelling ctx stops the walk between requests.
func (l *CollectionLister) Walk(ctx context.Context, root string, depth int, visit func(FileInfo), onError func(path string, err error)) error {
	if depth < 1 {
		return errors.Errorf("walk depth must be at least 1, got %d", depth)
	}
	return l.walk(ctx, root, depth, true, visit, onError)
}

func (l *CollectionLister) walk(ctx context.Context, dir string, depth int, isRoot bool, visit func(FileInfo), onError func(path string, err error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var infos []fs.FileInfo
	err := retryWebDavOperation("ReadDir", func() error {
		var err error
		infos, err = l.webdav.ReadDir(dir)
		return err
	})
	if err != nil {
		if gowebdav.IsErrNotFound(err) {
			err = errors.Wrap(ErrCollectionNotFound, dir)
		} else {
			statInfo, classified := classifyReadDirErr(l.webdav, dir, err)
			if classified == nil && statInfo != nil {
				// dir is a plain object, not a collection.
				visit(FileInfo{Name: dir, Size: statInfo.Size(), ModTime: statInfo.ModTime()})
				return nil
			}
			err = classified
		}
		if isRoot {
			return err
		}
		if onError != nil {
			onError(dir, err)
		}
		return nil
	}

	for _, info := range infos {
		child := path.Join(dir, info.Name())
		if !info.IsDir() {
			visit(FileInfo{Name: child, Size: info.Size(), ModTime: info.ModTime()})
			continue
		}
		if depth > 1 {
			// Failures below root go to onError, so only cancellation comes back.
			if err := l.walk(ctx, child, depth-1, false, visit, onError); err != nil {
				return err
			}
		}
	}
	return nil
}
