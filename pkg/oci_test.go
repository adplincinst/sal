package pkg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseArtifactDefaultsReferenceToLatest(t *testing.T) {
	ref, err := ParseArtifact("ghcr.io/my-username/my-repository")

	require.NoError(t, err)
	require.Equal(t, "ghcr.io/my-username/my-repository", ref.Repository)
	require.Equal(t, "latest", ref.Reference)
	require.Equal(t, "ghcr.io", ref.RegistryName)
	require.Equal(t, "my-username", ref.Owner)
	require.Equal(t, "my-repository", ref.ArtifactName)
}

func TestParseArtifactStripsHTTPScheme(t *testing.T) {
	ref, err := ParseArtifact("https://ghcr.io/my-username/my-repository:v1")

	require.NoError(t, err)
	require.Equal(t, "ghcr.io/my-username/my-repository", ref.Repository)
	require.Equal(t, "v1", ref.Reference)
	require.Equal(t, "ghcr.io", ref.RegistryName)
	require.Equal(t, "my-username", ref.Owner)
}

func TestRepoDirFromSourceHandlesSSHURLs(t *testing.T) {
	got := RepoDirFromSource("git@github.com:cgs-earth/sal.git")

	require.Equal(t, "sal", got)
}

func TestRepoDirFromSourceHandlesHTTPSURLs(t *testing.T) {
	got := RepoDirFromSource("https://github.com/cgs-earth/sal.git")

	require.Equal(t, "sal", got)
}
