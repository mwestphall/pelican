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

package server_structs

import (
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// A client subscribes to an origin's object notifications by sending a STOMP
// SUBSCRIBE whose destination, the topic, is
//
//	<client id>/<depth>/<protocol>/<object path>
//
// for example "client1/2/osdf/ns/dir".  The client id is the client's unique name
// for its subscription, which must match the SUBSCRIBE's "subscription" header.
// The depth is how many levels of the object path the client wants to hear about,
// at least 1: objects directly in the path are one level down.  The protocol is
// the scheme of the URL the client was given, e.g. osdf or pelican.
//
// The notification server hands everything after the client id to the origin as
// the subscription's parameters.  This file is where the topic is formatted by
// clients and read back by origins, so the two cannot disagree.

// SubscriptionTopic returns the topic with which the client named clientID
// subscribes to the objects within depth levels of objPath.
func SubscriptionTopic(clientID string, depth int, protocol, objPath string) (string, error) {
	if clientID == "" || strings.Contains(clientID, "/") {
		return "", errors.Errorf("subscription client id %q must be non-empty and contain no '/'", clientID)
	}
	if depth < 1 {
		return "", errors.Errorf("subscription depth must be at least 1, got %d", depth)
	}
	if protocol == "" || strings.Contains(protocol, "/") {
		return "", errors.Errorf("subscription protocol %q must be non-empty and contain no '/'", protocol)
	}
	return clientID + "/" + strconv.Itoa(depth) + "/" + protocol + path.Clean("/"+objPath), nil
}

// ParseSubscriptionParams reads the part of a topic after the client id:
// "<depth>/<protocol>/<object path>".  The object path it returns is cleaned and
// begins with "/"; it is "/" if the topic names the whole namespace.
func ParseSubscriptionParams(params string) (depth int, protocol, objPath string, err error) {
	depthStr, rest, found := strings.Cut(params, "/")
	if !found {
		return 0, "", "", errors.Errorf("subscription %q has no depth", params)
	}
	depth, err = strconv.Atoi(depthStr)
	if err != nil || depth < 1 {
		return 0, "", "", errors.Errorf("subscription %q has depth %q; it must be an integer of at least 1", params, depthStr)
	}
	protocol, objPath, found = strings.Cut(rest, "/")
	if !found || protocol == "" {
		return 0, "", "", errors.Errorf("subscription %q has no protocol", params)
	}
	return depth, protocol, path.Clean("/" + objPath), nil
}
