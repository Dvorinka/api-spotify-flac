package pipeline

import (
	"encoding/json"
	"time"
)

type LookupInput struct {
	URL string `json:"url"`
}

type CreateJobInput struct {
	Items               []string `json:"items"`
	IncludeOutputBase64 bool     `json:"include_output_base64,omitempty"`
	PreferLinks         bool     `json:"prefer_links,omitempty"`
}

type TrackInfo struct {
	SourceURL string `json:"source_url"`
	Platform  string `json:"platform"`
	Kind      string `json:"kind"`
	TrackID   string `json:"track_id"`
	Title     string `json:"title,omitempty"`
	Artist    string `json:"artist,omitempty"`
	Album     string `json:"album,omitempty"`
	CoverURL  string `json:"cover_url,omitempty"`
}

type JobItem struct {
	SourceURL       string    `json:"source_url"`
	Status          string    `json:"status"`
	Track           TrackInfo `json:"track"`
	DownloadURL     string    `json:"download_url,omitempty"`
	ExpiresAt       time.Time `json:"expires_at,omitempty"`
	Quality         string    `json:"quality,omitempty"`
	Source          string    `json:"source,omitempty"`
	OutputFilename  string    `json:"output_filename,omitempty"`
	OutputMime      string    `json:"output_mime,omitempty"`
	SizeBytes       int       `json:"size_bytes,omitempty"`
	OutputBase64    string    `json:"output_base64,omitempty"`
	Error           string    `json:"error,omitempty"`
	AttemptCount    int       `json:"attempt_count"`
	LastAttemptedAt time.Time `json:"last_attempted_at,omitempty"`
}

type Job struct {
	ID                  string     `json:"id"`
	Status              string     `json:"status"`
	CancelRequested     bool       `json:"cancel_requested,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	StartedAt           time.Time  `json:"started_at,omitempty"`
	FinishedAt          time.Time  `json:"finished_at,omitempty"`
	IncludeOutputBase64 bool       `json:"include_output_base64"`
	PreferLinks         bool       `json:"prefer_links,omitempty"`
	TotalItems          int        `json:"total_items"`
	CompletedItems      int        `json:"completed_items"`
	FailedItems         int        `json:"failed_items"`
	Items               []JobItem  `json:"items"`
	Events              []JobEvent `json:"events,omitempty"`
}

type ItemOutput struct {
	Filename string
	Mime     string
	Data     []byte
}

type JobEvent struct {
	Timestamp  time.Time `json:"timestamp"`
	Type       string    `json:"type"`
	Message    string    `json:"message,omitempty"`
	ItemIndex  int       `json:"item_index,omitempty"`
	ItemStatus string    `json:"item_status,omitempty"`
}

type WebhookMetrics struct {
	Attempts       int       `json:"attempts"`
	Successes      int       `json:"successes"`
	Failures       int       `json:"failures"`
	RetriedEvents  int       `json:"retried_events"`
	DeadLetters    int       `json:"dead_letters"`
	LastEvent      string    `json:"last_event,omitempty"`
	LastJobID      string    `json:"last_job_id,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	LastStatusCode int       `json:"last_status_code,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type WebhookFailure struct {
	Timestamp      time.Time       `json:"timestamp"`
	Event          string          `json:"event"`
	JobID          string          `json:"job_id"`
	URL            string          `json:"url"`
	Attempts       int             `json:"attempts"`
	LastStatusCode int             `json:"last_status_code,omitempty"`
	Error          string          `json:"error"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

type ReplayWebhookInput struct {
	Limit  int    `json:"limit,omitempty"`
	Before int    `json:"before,omitempty"`
	JobID  string `json:"job_id,omitempty"`
	Event  string `json:"event,omitempty"`
}

type WebhookReplayEntry struct {
	Event          string `json:"event"`
	JobID          string `json:"job_id"`
	Success        bool   `json:"success"`
	Attempts       int    `json:"attempts"`
	LastStatusCode int    `json:"last_status_code,omitempty"`
	Error          string `json:"error,omitempty"`
}

type ReplayWebhookResult struct {
	Total      int                  `json:"total"`
	Before     int                  `json:"before"`
	NextBefore int                  `json:"next_before"`
	Selected   int                  `json:"selected"`
	Attempted  int                  `json:"attempted"`
	Succeeded  int                  `json:"succeeded"`
	Failed     int                  `json:"failed"`
	Results    []WebhookReplayEntry `json:"results"`
}
