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
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustParseUrl(t *testing.T, s string) url.URL {
	t.Helper()
	u, err := url.Parse(s)
	require.NoError(t, err)
	return *u
}

func TestNotifyUrlFromWebUrl(t *testing.T) {
	testCases := []struct {
		name     string
		webUrl   string
		expected string
		ok       bool
	}{
		{"https becomes wss", "https://origin.example.com:8444", "wss://origin.example.com:8444" + OriginNotifyRoute, true},
		{"http becomes ws", "http://origin.example.com", "ws://origin.example.com" + OriginNotifyRoute, true},
		{"web url path is not kept", "https://origin.example.com/some/path", "wss://origin.example.com" + OriginNotifyRoute, true},
		{"unsupported scheme", "ftp://origin.example.com", "", false},
		{"no host", "https:///path", "", false},
		{"empty", "", "", false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := NotifyUrlFromWebUrl(mustParseUrl(t, tc.webUrl))
			assert.Equal(t, tc.ok, ok)
			if tc.ok {
				assert.Equal(t, tc.expected, got.String())
			}
		})
	}
}

func TestSetXNamespaceHeaderNotify(t *testing.T) {
	notifyServer := ServerAd{WebURL: mustParseUrl(t, "https://origin.example.com:8444"), Caps: Capabilities{Notify: true}}
	quietServer := ServerAd{WebURL: mustParseUrl(t, "https://quiet.example.com:8444")}
	notifyNs := NamespaceAd{Path: "/foo", Caps: Capabilities{PublicReads: true, Reads: true, Notify: true}}
	quietNs := NamespaceAd{Path: "/foo", Caps: Capabilities{PublicReads: true, Reads: true}}

	t.Run("requested and supported", func(t *testing.T) {
		hdr := http.Header{}
		SetXNamespaceHeader(hdr, []ServerAd{quietServer, notifyServer}, notifyNs, true)
		assert.Equal(t, "namespace=/foo, require-token=false, notify-url=wss://origin.example.com:8444"+OriginNotifyRoute,
			hdr[XPelNs{}.GetName()][0], "the first notify-capable origin supplies the URL")
	})
	t.Run("not requested", func(t *testing.T) {
		hdr := http.Header{}
		SetXNamespaceHeader(hdr, []ServerAd{notifyServer}, notifyNs, false)
		assert.Equal(t, "namespace=/foo, require-token=false", hdr[XPelNs{}.GetName()][0])
	})
	t.Run("namespace does not support it", func(t *testing.T) {
		hdr := http.Header{}
		SetXNamespaceHeader(hdr, []ServerAd{notifyServer}, quietNs, true)
		assert.Equal(t, "namespace=/foo, require-token=false", hdr[XPelNs{}.GetName()][0])
	})
	t.Run("no origin supports it", func(t *testing.T) {
		hdr := http.Header{}
		SetXNamespaceHeader(hdr, []ServerAd{quietServer}, notifyNs, true)
		assert.Equal(t, "namespace=/foo, require-token=false", hdr[XPelNs{}.GetName()][0])
	})
	t.Run("round trips through the parser", func(t *testing.T) {
		hdr := http.Header{}
		SetXNamespaceHeader(hdr, []ServerAd{notifyServer}, notifyNs, true)
		var parsed XPelNs
		require.NoError(t, parsed.ParseRawHeader(&hdr))
		assert.Equal(t, "/foo", parsed.Namespace)
		require.NotNil(t, parsed.NotifyUrl)
		assert.Equal(t, "wss://origin.example.com:8444"+OriginNotifyRoute, parsed.NotifyUrl.String())
	})
}

func TestCapabilitiesNotifyJSON(t *testing.T) {
	data, err := json.Marshal(Capabilities{Notify: true})
	require.NoError(t, err)
	assert.Contains(t, string(data), `"Notify":true`)

	// An ad from an origin that predates the capability has no such key.
	var caps Capabilities
	require.NoError(t, json.Unmarshal([]byte(`{"Read":true}`), &caps))
	assert.False(t, caps.Notify)
}
