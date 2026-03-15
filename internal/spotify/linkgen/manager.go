package linkgen

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type LinkManager struct {
	generators []LinkGenerator
	cache      map[string]*CachedLink
	cacheMu    sync.RWMutex
}

type CachedLink struct {
	Link      *DownloadLink
	CreatedAt time.Time
}

func NewLinkManager() *LinkManager {
	lm := &LinkManager{
		generators: []LinkGenerator{
			NewTidalLinkGenerator(),
			NewQobuzLinkGenerator(),
		},
		cache: make(map[string]*CachedLink),
	}

	// Start cache cleanup goroutine
	go lm.cleanupCache()

	return lm
}

func (lm *LinkManager) GetDownloadLink(ctx context.Context, spotifyTrackID string) (*DownloadLink, error) {
	// Check cache first
	if link := lm.getCachedLink(spotifyTrackID); link != nil {
		if time.Now().Before(link.Link.ExpiresAt) {
			return link.Link, nil
		}
		// Remove expired link
		lm.removeCachedLink(spotifyTrackID)
	}

	// Try each generator in random order
	generators := lm.shuffleGenerators()
	
	for _, generator := range generators {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		link, err := generator.GetDownloadLink(spotifyTrackID)
		if err != nil {
			continue
		}

		if link != nil {
			// Cache the link
			lm.cacheLink(spotifyTrackID, link)
			return link, nil
		}
	}

	return nil, fmt.Errorf("failed to get download link from any service")
}

func (lm *LinkManager) getCachedLink(trackID string) *CachedLink {
	lm.cacheMu.RLock()
	defer lm.cacheMu.RUnlock()
	
	if cached, exists := lm.cache[trackID]; exists {
		return cached
	}
	return nil
}

func (lm *LinkManager) cacheLink(trackID string, link *DownloadLink) {
	lm.cacheMu.Lock()
	defer lm.cacheMu.Unlock()
	
	lm.cache[trackID] = &CachedLink{
		Link:      link,
		CreatedAt: time.Now(),
	}
}

func (lm *LinkManager) removeCachedLink(trackID string) {
	lm.cacheMu.Lock()
	defer lm.cacheMu.Unlock()
	
	delete(lm.cache, trackID)
}

func (lm *LinkManager) shuffleGenerators() []LinkGenerator {
	generators := make([]LinkGenerator, len(lm.generators))
	copy(generators, lm.generators)
	
	rand.Shuffle(len(generators), func(i, j int) {
		generators[i], generators[j] = generators[j], generators[i]
	})
	
	return generators
}

func (lm *LinkManager) cleanupCache() {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		lm.cacheMu.Lock()
		now := time.Now()
		
		for trackID, cached := range lm.cache {
			if now.After(cached.Link.ExpiresAt) {
				delete(lm.cache, trackID)
			}
		}
		
		lm.cacheMu.Unlock()
	}
}

func (lm *LinkManager) GetCacheStats() map[string]interface{} {
	lm.cacheMu.RLock()
	defer lm.cacheMu.RUnlock()
	
	stats := map[string]interface{}{
		"total_cached": len(lm.cache),
		"generators":   len(lm.generators),
	}
	
	return stats
}
