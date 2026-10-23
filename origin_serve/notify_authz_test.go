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

package origin_serve

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/pelicanplatform/pelican/server_structs"
	"github.com/pelicanplatform/pelican/server_utils"
)

// Tokens are verified against the issuer's keys, and read access to a path comes
// from a public export or from a storage.read scope covering it.
func TestNotifyAuthorization(t *testing.T) {
	const issuer = "https://notify-issuer.example.com"
	key := generateTestKey(t)
	pub, err := key.PublicKey()
	require.NoError(t, err)
	keys := jwk.NewSet()
	require.NoError(t, keys.AddKey(pub))

	ctx, cancel := context.WithCancel(context.Background())
	egrp := &errgroup.Group{}
	exports := []server_utils.OriginExport{
		{
			FederationPrefix: "/private",
			IssuerUrls:       []string{issuer},
			Capabilities:     server_structs.Capabilities{Reads: true},
		},
		{
			FederationPrefix: "/public",
			Capabilities:     server_structs.Capabilities{Reads: true, PublicReads: true},
		},
	}
	require.NoError(t, InitAuthConfig(ctx, egrp, exports))
	t.Cleanup(func() {
		cancel()
		_ = egrp.Wait()
		globalAuthConfig = nil
	})
	// Skip the JWKS lookup: the issuer's keys are already known.
	GetAuthConfig().issuerKeys.Set(issuer, authConfigItem{set: keys}, ttlcache.DefaultTTL)

	readDir := createTestToken(t, key, issuer, "alice", nil, "storage.read:/dir")
	otherKey := generateTestKey(t)
	forged := createTestToken(t, otherKey, issuer, "mallory", nil, "storage.read:/")

	t.Run("ValidTokenVerifies", func(t *testing.T) {
		user, valid, expiry := VerifyTokens([]string{forged, readDir})
		assert.Equal(t, "alice", user)
		assert.Equal(t, []string{readDir}, valid)
		assert.WithinDuration(t, time.Now().Add(time.Hour), expiry, 10*time.Second)
	})
	t.Run("ForgedTokenDoesNot", func(t *testing.T) {
		_, valid, expiry := VerifyTokens([]string{forged})
		assert.Empty(t, valid)
		assert.True(t, expiry.IsZero())
	})
	t.Run("ScopeCoversPath", func(t *testing.T) {
		assert.True(t, AuthorizeRead("/private/dir", []string{readDir}))
		assert.True(t, AuthorizeRead("/private/dir/sub/obj", []string{readDir}))
	})
	t.Run("ScopeDoesNotCoverOtherPaths", func(t *testing.T) {
		assert.False(t, AuthorizeRead("/private", []string{readDir}))
		assert.False(t, AuthorizeRead("/private/dirty", []string{readDir}))
		assert.False(t, AuthorizeRead("/private/dir/../secret", []string{readDir}))
	})
	t.Run("NoTokenCannotReadPrivate", func(t *testing.T) {
		assert.False(t, AuthorizeRead("/private/dir", nil))
	})
	t.Run("NoTokenCanReadPublic", func(t *testing.T) {
		assert.True(t, AuthorizeRead("/public/anything", nil))
		assert.False(t, AuthorizeRead("/", nil))
	})
	t.Run("UnverifiedTokenAuthorizesNothing", func(t *testing.T) {
		assert.False(t, AuthorizeRead("/private/dir", []string{forged}))
	})
	t.Run("ExtractTokens", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/?authz=Bearer%20"+readDir, nil)
		assert.Equal(t, []string{readDir}, ExtractTokens(r))
	})
}

// Without an initialized auth config nothing is authorized, rather than everything.
func TestNotifyAuthorizationUninitialized(t *testing.T) {
	old := globalAuthConfig
	globalAuthConfig = nil
	t.Cleanup(func() { globalAuthConfig = old })
	user, valid, _ := VerifyTokens([]string{"x"})
	assert.Empty(t, user)
	assert.Empty(t, valid)
	assert.False(t, AuthorizeRead("/public/x", nil))
}
