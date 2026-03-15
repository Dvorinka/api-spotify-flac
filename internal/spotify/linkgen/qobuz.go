package linkgen

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type QobuzLinkGenerator struct {
	client *http.Client
	appID  string
}

type QobuzSearchResponse struct {
	Query  string `json:"query"`
	Tracks struct {
		Limit  int          `json:"limit"`
		Offset int          `json:"offset"`
		Total  int          `json:"total"`
		Items  []QobuzTrack `json:"items"`
	} `json:"tracks"`
}

type QobuzTrack struct {
	ID                  int64   `json:"id"`
	Title               string  `json:"title"`
	Version             string  `json:"version"`
	Duration            int     `json:"duration"`
	TrackNumber         int     `json:"track_number"`
	MediaNumber         int     `json:"media_number"`
	ISRC                string  `json:"isrc"`
	Copyright           string  `json:"copyright"`
	MaximumBitDepth     int     `json:"maximum_bit_depth"`
	MaximumSamplingRate float64 `json:"maximum_sampling_rate"`
	Hires               bool    `json:"hires"`
	HiresStreamable     bool    `json:"hires_streamable"`
	ReleaseDateOriginal string  `json:"release_date_original"`
	Performer           struct {
		Name string `json:"name"`
		ID   int64  `json:"id"`
	} `json:"performer"`
	Album struct {
		Title string `json:"title"`
		ID    string `json:"id"`
		Image struct {
			Small     string `json:"small"`
			Thumbnail string `json:"thumbnail"`
			Large     string `json:"large"`
		} `json:"image"`
		Artist struct {
			Name string `json:"name"`
			ID   int64  `json:"id"`
		} `json:"artist"`
		Label struct {
			Name string `json:"name"`
		} `json:"label"`
	} `json:"album"`
}

type QobuzStreamResponse struct {
	URL string `json:"url"`
}

func NewQobuzLinkGenerator() *QobuzLinkGenerator {
	return &QobuzLinkGenerator{
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
		appID: "798273057",
	}
}

func (q *QobuzLinkGenerator) searchByISRC(isrc string) (*QobuzTrack, error) {
	apiBase := "https://www.qobuz.com/api.json/0.2/track/search?query="
	url := fmt.Sprintf("%s%s&limit=1&app_id=%s", apiBase, isrc, q.appID)

	resp, err := q.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to search track: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var searchResp QobuzSearchResponse
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if len(body) == 0 {
		return nil, fmt.Errorf("API returned empty response")
	}

	if err := json.Unmarshal(body, &searchResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(searchResp.Tracks.Items) == 0 {
		return nil, fmt.Errorf("track not found for ISRC: %s", isrc)
	}

	return &searchResp.Tracks.Items[0], nil
}

func (q *QobuzLinkGenerator) GetDownloadURL(trackID int64, quality string) (string, error) {
	standardAPIs := []string{
		"https://dab.yeet.su/api/stream?trackId=",
		"https://api.qobuz.su/api/stream?trackId=",
		"https://stream.qobuz.su/api/stream?trackId=",
	}

	qualityCode := quality
	if qualityCode == "" || qualityCode == "5" {
		qualityCode = "6"
	}

	for _, apiBase := range standardAPIs {
		apiURL := fmt.Sprintf("%s%d&quality=%s", apiBase, trackID, qualityCode)

		resp, err := q.client.Get(apiURL)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			continue
		}

		if len(body) == 0 {
			continue
		}

		var streamResp QobuzStreamResponse
		if err := json.Unmarshal(body, &streamResp); err == nil && streamResp.URL != "" {
			return streamResp.URL, nil
		}

		var nestedResp struct {
			Data struct {
				URL string `json:"url"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &nestedResp); err == nil && nestedResp.Data.URL != "" {
			return nestedResp.Data.URL, nil
		}
	}

	return "", fmt.Errorf("failed to get download URL from all APIs")
}

func (q *QobuzLinkGenerator) GetDownloadLink(spotifyTrackID string) (*DownloadLink, error) {
	// First try to get ISRC from Spotify metadata
	// For now, we'll use a simplified approach
	track, err := q.searchByISRC(spotifyTrackID)
	if err != nil {
		// Try searching by title/artist if ISRC fails
		return nil, fmt.Errorf("failed to find track: %w", err)
	}

	qualities := []string{"27", "7", "6"} // Hi-res, Lossless, High quality
	for _, quality := range qualities {
		downloadURL, err := q.GetDownloadURL(track.ID, quality)
		if err == nil && downloadURL != "" {
			qualityLabel := "lossless"
			if quality == "27" {
				qualityLabel = "hires"
			} else if quality == "7" {
				qualityLabel = "cd"
			}

			return &DownloadLink{
				URL:           downloadURL,
				Quality:       qualityLabel,
				Source:        "qobuz",
				ExpiresAt:     time.Now().Add(4 * time.Hour),
				TrackID:       spotifyTrackID,
				SourceTrackID: strconv.FormatInt(track.ID, 10),
				BitDepth:      track.MaximumBitDepth,
				SampleRate:    int(track.MaximumSamplingRate),
			}, nil
		}
	}

	return nil, fmt.Errorf("failed to get download URL for any quality")
}
