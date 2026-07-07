package push

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPushReturnsErrorBeforeStartingUploadsWhenDataDirIsEmpty(t *testing.T) {
	err := push(context.Background(), t.TempDir(), nil, "example.com/repo")

	require.Error(t, err)
	require.Contains(t, err.Error(), "no files found in SAL data directory")
}

func TestFormatUploadedSizeUsesKBForSmallUploads(t *testing.T) {
	got := formatUploadedSize(512 * 1024)

	require.Equal(t, "512.00 KB", got)
}

func TestFormatUploadedSizeUsesMBForLargeUploads(t *testing.T) {
	got := formatUploadedSize(2 * 1024 * 1024)

	require.Equal(t, "2.00 MB", got)
}
