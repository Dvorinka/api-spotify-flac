package pipeline

import (
	"context"
	"apiservices/spotify-flac/internal/spotify/linkgen"
)

func (s *Service) GetDownloadLink(ctx context.Context, trackID string) (*linkgen.DownloadLink, error) {
	return s.linkManager.GetDownloadLink(ctx, trackID)
}

func (s *Service) GetLinkCacheStats() map[string]interface{} {
	return s.linkManager.GetCacheStats()
}
