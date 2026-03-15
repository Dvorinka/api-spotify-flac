package linkgen

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type TidalLinkGenerator struct {
	client     *http.Client
	timeout    time.Duration
	maxRetries int
	apiURLs    []string
}

type TidalAPIResponse struct {
	OriginalTrackURL string `json:"OriginalTrackUrl"`
}

type TidalAPIResponseV2 struct {
	Version string `json:"version"`
	Data    struct {
		TrackID           int64  `json:"trackId"`
		AssetPresentation string `json:"assetPresentation"`
		AudioMode         string `json:"audioMode"`
		AudioQuality      string `json:"audioQuality"`
		ManifestMimeType  string `json:"manifestMimeType"`
		ManifestHash      string `json:"manifestHash"`
		Manifest          string `json:"manifest"`
		BitDepth          int    `json:"bitDepth"`
		SampleRate        int    `json:"sampleRate"`
	} `json:"data"`
}

type TidalBTSManifest struct {
	MimeType       string   `json:"mimeType"`
	Codecs         string   `json:"codecs"`
	EncryptionType string   `json:"encryptionType"`
	URLs           []string `json:"urls"`
}

func NewTidalLinkGenerator() *TidalLinkGenerator {
	return &TidalLinkGenerator{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		timeout:    30 * time.Second,
		maxRetries: 3,
		apiURLs: []string{
			"https://hifi-one.spotisaver.net",
			"https://hifi-two.spotisaver.net",
			"https://eu-central.monochrome.tf",
			"https://us-west.monochrome.tf",
			"https://api.monochrome.tf",
			"https://monochrome-api.samidy.com",
			"https://tidal.kinoplus.online",
		},
	}
}

func (t *TidalLinkGenerator) GetTidalURLFromSpotify(spotifyTrackID string) (string, error) {
	spotifyBase := "https://open.spotify.com/track/"
	spotifyURL := fmt.Sprintf("%s%s", spotifyBase, spotifyTrackID)

	apiBase := "https://api.song.link/v1-alpha.1/links?url="
	apiURL := fmt.Sprintf("%s%s", apiBase, url.QueryEscape(spotifyURL))

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36")

	resp, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to get Tidal URL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var songLinkResp struct {
		LinksByPlatform map[string]struct {
			URL string `json:"url"`
		} `json:"linksByPlatform"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&songLinkResp); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	tidalLink, ok := songLinkResp.LinksByPlatform["tidal"]
	if !ok || tidalLink.URL == "" {
		return "", fmt.Errorf("tidal link not found")
	}

	return tidalLink.URL, nil
}

func (t *TidalLinkGenerator) GetTrackIDFromURL(tidalURL string) (int64, error) {
	parts := strings.Split(tidalURL, "/track/")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid tidal URL format")
	}

	trackIDStr := strings.Split(parts[1], "?")[0]
	trackIDStr = strings.TrimSpace(trackIDStr)

	var trackID int64
	_, err := fmt.Sscanf(trackIDStr, "%d", &trackID)
	if err != nil {
		return 0, fmt.Errorf("failed to parse track ID: %w", err)
	}

	return trackID, nil
}

func (t *TidalLinkGenerator) GetDownloadURL(trackID int64, quality string) (string, error) {
	for attempt, apiURL := range t.apiURLs {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		url := fmt.Sprintf("%s/track/?id=%d&quality=%s", apiURL, trackID, quality)

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			continue
		}

		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36")

		resp, err := t.client.Do(req)
		if err != nil {
			continue
		}

		if resp.StatusCode == 200 {
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				continue
			}

			var apiResp TidalAPIResponse
			if err := json.Unmarshal(body, &apiResp); err == nil && apiResp.OriginalTrackURL != "" {
				return apiResp.OriginalTrackURL, nil
			}

			var apiRespV2 TidalAPIResponseV2
			if err := json.Unmarshal(body, &apiRespV2); err == nil {
				if apiRespV2.Data.ManifestMimeType == "application/dash+xml" {
					var manifest TidalBTSManifest
					if err := json.Unmarshal([]byte(apiRespV2.Data.Manifest), &manifest); err == nil && len(manifest.URLs) > 0 {
						return manifest.URLs[0], nil
					}
				}
			}
		}
		resp.Body.Close()
	}

	return "", fmt.Errorf("failed to get download URL after %d attempts", len(t.apiURLs))
}

func (t *TidalLinkGenerator) GetDownloadLink(spotifyTrackID string) (*DownloadLink, error) {
	tidalURL, err := t.GetTidalURLFromSpotify(spotifyTrackID)
	if err != nil {
		return nil, fmt.Errorf("failed to get Tidal URL: %w", err)
	}

	trackID, err := t.GetTrackIDFromURL(tidalURL)
	if err != nil {
		return nil, fmt.Errorf("failed to extract track ID: %w", err)
	}

	qualities := []string{"lossless", "high", "medium"}
	for _, quality := range qualities {
		downloadURL, err := t.GetDownloadURL(trackID, quality)
		if err == nil && downloadURL != "" {
			return &DownloadLink{
				URL:           downloadURL,
				Quality:       quality,
				Source:        "tidal",
				ExpiresAt:     time.Now().Add(4 * time.Hour),
				TrackID:       spotifyTrackID,
				SourceTrackID: strconv.FormatInt(trackID, 10),
			}, nil
		}
	}

	return nil, fmt.Errorf("failed to get download URL for any quality")
}
