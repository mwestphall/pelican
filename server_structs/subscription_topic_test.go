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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionTopic(t *testing.T) {
	testCases := []struct {
		clientID string
		depth    int
		protocol string
		objPath  string
		expected string
	}{
		{"client1", 2, "osdf", "/ns/dir", "client1/2/osdf/ns/dir"},
		{"client1", 1, "pelican", "/ns/dir/", "client1/1/pelican/ns/dir"},
		{"client1", 1, "pelican", "ns/dir", "client1/1/pelican/ns/dir"},
		{"client1", 5, "osdf", "/", "client1/5/osdf/"},
		{"client1", 5, "osdf", "", "client1/5/osdf/"},
		{"client1", 1, "osdf", "/a/../b//c", "client1/1/osdf/b/c"},
	}
	for _, tc := range testCases {
		topic, err := SubscriptionTopic(tc.clientID, tc.depth, tc.protocol, tc.objPath)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, topic)
	}
}

func TestSubscriptionTopicRejectsWhatCannotBeRead(t *testing.T) {
	for _, tc := range []struct {
		name     string
		clientID string
		depth    int
		protocol string
	}{
		{"empty client id", "", 1, "osdf"},
		{"client id with a slash", "a/b", 1, "osdf"},
		{"zero depth", "c", 0, "osdf"},
		{"negative depth", "c", -1, "osdf"},
		{"empty protocol", "c", 1, ""},
		{"protocol with a slash", "c", 1, "os/df"},
	} {
		_, err := SubscriptionTopic(tc.clientID, tc.depth, tc.protocol, "/ns")
		assert.Error(t, err, tc.name)
	}
}

// What a client formats is what an origin reads back.
func TestSubscriptionTopicRoundTrips(t *testing.T) {
	for _, objPath := range []string{"/ns/dir", "/", "/a/b/c/d", "/with space/and%20percent"} {
		topic, err := SubscriptionTopic("client1", 4, "osdf", objPath)
		require.NoError(t, err)

		clientID, params, found := strings.Cut(topic, "/")
		require.True(t, found)
		assert.Equal(t, "client1", clientID)
		depth, protocol, gotPath, err := ParseSubscriptionParams(params)
		require.NoError(t, err, topic)
		assert.Equal(t, 4, depth)
		assert.Equal(t, "osdf", protocol)
		assert.Equal(t, objPath, gotPath)
	}
}

func TestParseSubscriptionParams(t *testing.T) {
	depth, protocol, objPath, err := ParseSubscriptionParams("2/osdf/ns/dir")
	require.NoError(t, err)
	assert.Equal(t, 2, depth)
	assert.Equal(t, "osdf", protocol)
	assert.Equal(t, "/ns/dir", objPath)

	_, _, objPath, err = ParseSubscriptionParams("1/pelican/")
	require.NoError(t, err)
	assert.Equal(t, "/", objPath, "no object path means the whole namespace")

	for _, params := range []string{"", "osdf/ns/dir", "x/osdf/ns", "0/osdf/ns", "-1/osdf/ns", "1.5/osdf/ns", "2/osdf", "2", "2//ns"} {
		_, _, _, err := ParseSubscriptionParams(params)
		assert.Error(t, err, "%q", params)
	}
}
