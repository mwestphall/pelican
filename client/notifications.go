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
	"net/http"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	stompclient "github.com/sci-ndp/pelican-client-protocol/stomp-server-go/client"

	"github.com/pelicanplatform/pelican/config"
)

const (
	// notifyDialTimeout bounds opening the websocket.
	notifyDialTimeout = 30 * time.Second
	// notifyDisconnectTimeout bounds how long closing the subscription waits for
	// the origin to acknowledge it, once the caller has had enough.
	notifyDisconnectTimeout = 5 * time.Second
)

// Notification is an origin's announcement that an object was created or modified.
type Notification struct {
	// Path is the object's federation path, e.g. /ns/dir/obj.
	Path string
	// ReceivedAt is when the announcement arrived.
	ReceivedAt time.Time
}

// Listen subscribes to r.Topic at the origin whose notification endpoint is
// r.NotifyUrl and calls onNotification for each object the origin announces,
// acknowledging each once onNotification has returned.  An announcement that is
// not acknowledged is sent again, so a callback that panics or a process that dies
// does not lose events.
//
// Listen blocks.  It returns nil once ctx is cancelled, and an error if the
// connection cannot be made, the origin refuses the subscription, or the origin
// ends the connection.  The origin keeps a subscription's undelivered events under
// its client id, so listening again with the same r.ClientID picks up where this
// left off.
func (r SubscribeResult) Listen(ctx context.Context, onNotification func(Notification)) error {
	if r.NotifyUrl == "" || r.Topic == "" || r.ClientID == "" {
		return errors.New("cannot listen without a notification endpoint, topic and client id")
	}

	// Dial the websocket with the same TLS and proxy settings as the rest of the
	// client, so that a federation with its own CA, or one run with TLS
	// verification off, behaves as it does for transfers.
	transport := config.GetTransport()
	dialer := websocket.Dialer{
		Proxy:            transport.Proxy,
		HandshakeTimeout: notifyDialTimeout,
	}
	if transport.TLSClientConfig != nil {
		dialer.TLSClientConfig = transport.TLSClientConfig.Clone()
	}
	var header http.Header
	if r.token != "" {
		header = http.Header{"Authorization": []string{"Bearer " + r.token}}
	}
	ws, resp, err := dialer.DialContext(ctx, r.NotifyUrl, header)
	if err != nil {
		if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			return errors.Wrapf(err, "the origin's notification endpoint %s refused the credential (HTTP %d)", r.NotifyUrl, resp.StatusCode)
		}
		return errors.Wrapf(err, "failed to connect to the origin's notification endpoint %s", r.NotifyUrl)
	}

	conn, err := stompclient.Connect(stompclient.NetConn(ws), stomp.ConnOpt.RcvReceiptTimeout(notifyDisconnectTimeout))
	if err != nil {
		return errors.Wrapf(err, "failed to open a notification session with %s", r.NotifyUrl)
	}

	// The origin matches the SUBSCRIBE's "subscription" header to the client id the
	// topic starts with, and keeps the subscription's queue under it.
	sub, err := conn.Subscribe(r.Topic, stomp.AckClientIndividual,
		stomp.SubscribeOpt.Id(r.ClientID),
		stomp.SubscribeOpt.Header("subscription", r.ClientID))
	if err != nil {
		conn.MustDisconnect()
		return errors.Wrapf(err, "failed to subscribe to %s", r.Topic)
	}

	for {
		select {
		case <-ctx.Done():
			// Closing a connection the origin has stopped answering must not hang us.
			if err := conn.Disconnect(); err != nil {
				conn.MustDisconnect()
			}
			return nil
		case msg, ok := <-sub.C:
			if !ok {
				return errors.Errorf("the origin at %s closed the connection", r.NotifyUrl)
			}
			if msg.Err != nil {
				// The origin sent an ERROR frame, or the connection to it failed.
				conn.MustDisconnect()
				return errors.Wrapf(msg.Err, "the subscription to %s at %s ended", r.Topic, r.NotifyUrl)
			}

			onNotification(Notification{Path: notifiedPath(msg.Body), ReceivedAt: time.Now()})
			if err := conn.Ack(msg); err != nil {
				conn.MustDisconnect()
				return errors.Wrap(err, "failed to acknowledge a notification")
			}
		}
	}
}

// notifiedPath reads the object path out of a notification's body, which the
// notification server sends as a JSON string.  A body that is not one is returned
// as it is rather than lost.
func notifiedPath(body []byte) string {
	var objPath string
	if err := json.Unmarshal(body, &objPath); err != nil {
		return string(body)
	}
	return objPath
}
