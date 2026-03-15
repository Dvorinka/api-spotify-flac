package linkgen

import "time"

type DownloadLink struct {
	URL            string    `json:"url"`
	Quality        string    `json:"quality"`
	Source         string    `json:"source"`
	ExpiresAt      time.Time `json:"expires_at"`
	TrackID        string    `json:"track_id"`
	SourceTrackID  string    `json:"source_track_id"`
	FileSize       int64     `json:"file_size,omitempty"`
	MimeType       string    `json:"mime_type,omitempty"`
	BitDepth       int       `json:"bit_depth,omitempty"`
	SampleRate     int       `json:"sample_rate,omitempty"`
}

type LinkGenerator interface {
	GetDownloadLink(spotifyTrackID string) (*DownloadLink, error)
}

type TrackMetadata struct {
	ID          string            `json:"id"`
	Title       string            `json:"title"`
	Artist      string            `json:"artist"`
	Album       string            `json:"album"`
	Duration    int               `json:"duration"`
	ISRC        string            `json:"isrc,omitempty"`
	ReleaseDate string            `json:"release_date,omitempty"`
	Genre       string            `json:"genre,omitempty"`
	TrackNumber int               `json:"track_number,omitempty"`
	CoverURL    string            `json:"cover_url,omitempty"`
	Extra       map[string]string `json:"extra,omitempty"`
}
