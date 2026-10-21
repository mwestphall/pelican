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
	"net/url"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/pelicanplatform/pelican/pelican_url"
	"github.com/pelicanplatform/pelican/server_structs"
)

// SubscribeResult is what the director says about subscribing to object
// notifications under a path, and the topic to subscribe with.  There is no Go
// notification client yet, so this is where a subscription stops: the caller learns
// which origins to connect to and what to ask them for.
type SubscribeResult struct {
	// Namespace is the federation prefix that covers the requested path.
	Namespace string `json:"namespace"`
	// RequireToken is whether the namespace requires a token to read.
	RequireToken bool `json:"requireToken"`
	// NotifyUrl is the websocket endpoint of the best-ranked origin.
	NotifyUrl string `json:"notifyUrl"`
	// Origins are the origins the director redirected to, best first.
	Origins []string `json:"origins"`
	// ClientID names the subscription.  A subscriber that reconnects with the same
	// one resumes the events it has not yet acknowledged.
	ClientID string `json:"clientId"`
	// Depth is how many levels below the requested path the subscription covers.
	Depth int `json:"depth"`
	// Topic is the STOMP destination that subscribes to the requested path and
	// depth: "<client id>/<depth>/<protocol>/<path>".  The SUBSCRIBE's
	// "subscription" header must be the client id.
	Topic string `json:"topic"`
}

// DoSubscribe asks the director which origins can notify a subscriber about
// objects under remoteObject, and works out the topic to subscribe with for the
// given depth: objects directly in remoteObject are one level down, so depth must
// be at least 1.  clientID names the subscription and must not contain '/'; if it
// is empty a random one is made.
//
// Like a listing it is a question for origins only; the director answers with
// redirects and the notify-url of the best origin, and fails if no origin exporting
// the path advertises the Notify capability.
func DoSubscribe(ctx context.Context, remoteObject string, depth int, clientID string) (SubscribeResult, error) {
	if clientID == "" {
		clientID = uuid.NewString()
	}
	if depth < 1 {
		return SubscribeResult{}, errors.Errorf("subscription depth must be at least 1, got %d", depth)
	}

	pUrl, err := ParseRemoteAsPUrl(ctx, remoteObject)
	if err != nil {
		return SubscribeResult{}, errors.Wrapf(err, "failed to parse remote path: %s", remoteObject)
	}
	topic, err := subscriptionTopicFor(pUrl, clientID, depth)
	if err != nil {
		return SubscribeResult{}, err
	}

	query := url.Values(pUrl.Query())
	query.Set(pelican_url.QueryNotify, "")
	pUrl.RawQuery = query.Encode()

	dirResp, err := getDirectorInfoForPath(ctx, pUrl, http.MethodGet, "", false)
	if err != nil {
		return SubscribeResult{}, err
	}
	if dirResp.XPelNsHdr.NotifyUrl == nil {
		return SubscribeResult{}, errors.Errorf("the director did not provide a notification endpoint for %s; no origin for this namespace may support notifications", pUrl.Path)
	}

	origins := make([]string, 0, len(dirResp.ObjectServers))
	for _, server := range dirResp.ObjectServers {
		origins = append(origins, originOf(server))
	}
	return SubscribeResult{
		Namespace:    dirResp.XPelNsHdr.Namespace,
		RequireToken: dirResp.XPelNsHdr.RequireToken,
		NotifyUrl:    dirResp.XPelNsHdr.NotifyUrl.String(),
		Origins:      origins,
		ClientID:     clientID,
		Depth:        depth,
		Topic:        topic,
	}, nil
}

// subscriptionTopicFor returns the topic that subscribes the client named
// clientID to the object path of pUrl, to the given depth.  Its protocol is the
// URL's scheme, e.g. osdf or pelican; any query on the URL is not part of the
// topic.
func subscriptionTopicFor(pUrl *pelican_url.PelicanURL, clientID string, depth int) (string, error) {
	protocol := pUrl.Scheme
	if protocol == "" {
		protocol = pelican_url.PelicanScheme
	}
	return server_structs.SubscriptionTopic(clientID, depth, protocol, pUrl.Path)
}

// originOf reduces a redirect URL, which carries the object path, to the
// scheme and host of the server it points at.
func originOf(u *url.URL) string {
	return (&url.URL{Scheme: u.Scheme, Host: u.Host}).String()
}
