package pipeline

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	reSpotifyWeb = regexp.MustCompile(`^https?://open\.spotify\.com/(track|album|playlist)/([A-Za-z0-9]+)`) //nolint:lll
	reSpotifyURI = regexp.MustCompile(`^spotify:(track|album|playlist):([A-Za-z0-9]+)$`)
)

const maxJobEvents = 500

type Config struct {
	DownloaderCmd  string
	OutputDir      string
	WorkerCount    int
	LookupBaseURL  string
	StateFile      string
	WebhookURL     string
	WebhookSecret  string
	WebhookRetries int
	WebhookRetryMS int
}

type Service struct {
	downloaderCmd  string
	outputDir      string
	lookupBaseURL  string
	stateFile      string
	webhookURL     string
	webhookSecret  string
	webhookRetries int
	webhookDelay   time.Duration
	webhookDLQFile string
	webhookMu      sync.Mutex
	webhookStatsMu sync.RWMutex
	webhookStats   WebhookMetrics
	httpClient     *http.Client
	nowFn          func() time.Time

	jobs       map[string]*Job
	jobCancels map[string]context.CancelFunc
	mu         sync.RWMutex
	queue      chan string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	counter uint64
}

type spotifyRef struct {
	Kind      string
	ID        string
	Canonical string
}

type persistedState struct {
	Counter uint64 `json:"counter"`
	Jobs    []Job  `json:"jobs"`
}

func NewService(cfg Config) (*Service, error) {
	outputDir := strings.TrimSpace(cfg.OutputDir)
	if outputDir == "" {
		outputDir = filepath.Join(os.TempDir(), "apiservices-spotify")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed creating output dir: %w", err)
	}

	workerCount := cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > 8 {
		workerCount = 8
	}

	lookupBase := strings.TrimSpace(cfg.LookupBaseURL)
	if lookupBase == "" {
		lookupBase = "https://open.spotify.com"
	}
	lookupBase = strings.TrimRight(lookupBase, "/")

	stateFile := strings.TrimSpace(cfg.StateFile)
	if stateFile == "" {
		stateFile = filepath.Join(outputDir, "jobs_state.json")
	}
	if err := os.MkdirAll(filepath.Dir(stateFile), 0o755); err != nil {
		return nil, fmt.Errorf("failed creating state dir: %w", err)
	}

	webhookRetries := cfg.WebhookRetries
	if webhookRetries <= 0 {
		webhookRetries = 3
	}
	if webhookRetries > 10 {
		webhookRetries = 10
	}

	webhookRetryMS := cfg.WebhookRetryMS
	if webhookRetryMS <= 0 {
		webhookRetryMS = 400
	}
	if webhookRetryMS > 10000 {
		webhookRetryMS = 10000
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		downloaderCmd:  strings.TrimSpace(cfg.DownloaderCmd),
		outputDir:      outputDir,
		lookupBaseURL:  lookupBase,
		stateFile:      stateFile,
		webhookURL:     strings.TrimSpace(cfg.WebhookURL),
		webhookSecret:  cfg.WebhookSecret,
		webhookRetries: webhookRetries,
		webhookDelay:   time.Duration(webhookRetryMS) * time.Millisecond,
		webhookDLQFile: filepath.Join(filepath.Dir(stateFile), "webhook_failures.jsonl"),
		httpClient:     &http.Client{Timeout: 10 * time.Second},
		nowFn:          func() time.Time { return time.Now().UTC() },
		jobs:           make(map[string]*Job),
		jobCancels:     make(map[string]context.CancelFunc),
		queue:          make(chan string, 1024),
		ctx:            ctx,
		cancel:         cancel,
	}

	queuedOnBoot := s.loadState()

	for i := 0; i < workerCount; i++ {
		s.wg.Add(1)
		go s.worker()
	}
	for _, jobID := range queuedOnBoot {
		s.enqueue(jobID)
	}

	return s, nil
}

func (s *Service) Close() {
	s.cancel()
	s.wg.Wait()
}

func (s *Service) Lookup(ctx context.Context, input LookupInput) (TrackInfo, error) {
	ref, err := parseSpotifyReference(input.URL)
	if err != nil {
		return TrackInfo{}, err
	}
	track := TrackInfo{
		SourceURL: ref.Canonical,
		Platform:  "spotify",
		Kind:      ref.Kind,
		TrackID:   ref.ID,
		Title:     strings.ToUpper(ref.Kind) + " " + ref.ID,
	}

	s.enrichFromOEmbed(ctx, &track)
	return track, nil
}

func (s *Service) CreateJob(_ context.Context, input CreateJobInput) (Job, error) {
	if len(input.Items) == 0 {
		return Job{}, errors.New("items cannot be empty")
	}
	if len(input.Items) > 100 {
		return Job{}, errors.New("max 100 items per job")
	}

	now := s.nowFn()
	jobID := s.nextJobID()
	job := &Job{
		ID:                  jobID,
		Status:              "queued",
		CreatedAt:           now,
		UpdatedAt:           now,
		IncludeOutputBase64: input.IncludeOutputBase64,
		TotalItems:          len(input.Items),
		Items:               make([]JobItem, 0, len(input.Items)),
	}

	for _, raw := range input.Items {
		itemURL := strings.TrimSpace(raw)
		job.Items = append(job.Items, JobItem{
			SourceURL: itemURL,
			Status:    "queued",
		})
	}

	s.mu.Lock()
	s.jobs[jobID] = job
	s.appendJobEventLocked(job, "job.created", "job queued", 0, "queued")
	s.mu.Unlock()
	s.persist()

	s.enqueue(jobID)
	return cloneJob(job), nil
}

func (s *Service) GetJob(jobID string) (Job, error) {
	s.mu.RLock()
	job, ok := s.jobs[jobID]
	s.mu.RUnlock()
	if !ok {
		return Job{}, errors.New("job not found")
	}
	return cloneJob(job), nil
}

func (s *Service) GetJobEvents(jobID string, limit int) ([]JobEvent, error) {
	events, _, _, err := s.GetJobEventsPage(jobID, limit, 0)
	return events, err
}

func (s *Service) GetWebhookMetrics() WebhookMetrics {
	s.webhookStatsMu.RLock()
	stats := s.webhookStats
	s.webhookStatsMu.RUnlock()
	return stats
}

func (s *Service) ListWebhookFailures(limit int) ([]WebhookFailure, error) {
	entries, _, _, err := s.GetWebhookFailuresPage(limit, 0, "", "")
	return entries, err
}

func (s *Service) GetWebhookFailuresPage(limit int, before int, jobID string, event string) ([]WebhookFailure, int, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}

	file, err := os.Open(s.webhookDLQFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []WebhookFailure{}, 0, 0, nil
		}
		return nil, 0, 0, err
	}
	defer file.Close()

	filterJobID := strings.TrimSpace(jobID)
	filterEvent := strings.TrimSpace(event)
	all := make([]WebhookFailure, 0, limit)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 4<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry WebhookFailure
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if filterJobID != "" && entry.JobID != filterJobID {
			continue
		}
		if filterEvent != "" && entry.Event != filterEvent {
			continue
		}
		all = append(all, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, 0, err
	}

	total := len(all)
	end := total
	if before > 0 && before < end {
		end = before
	}
	if before < 0 {
		before = 0
	}
	start := 0
	if end > limit {
		start = end - limit
	}
	page := make([]WebhookFailure, end-start)
	copy(page, all[start:end])
	nextBefore := start
	if nextBefore < 0 {
		nextBefore = 0
	}
	return page, total, nextBefore, nil
}

func (s *Service) ReplayWebhookFailures(ctx context.Context, input ReplayWebhookInput) (ReplayWebhookResult, error) {
	entries, total, nextBefore, err := s.GetWebhookFailuresPage(input.Limit, input.Before, input.JobID, input.Event)
	if err != nil {
		return ReplayWebhookResult{}, err
	}

	result := ReplayWebhookResult{
		Total:      total,
		Before:     input.Before,
		NextBefore: nextBefore,
		Selected:   len(entries),
		Results:    make([]WebhookReplayEntry, 0, len(entries)),
	}
	for i := range entries {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			default:
			}
		}

		entry := entries[i]
		itemResult := WebhookReplayEntry{
			Event: entry.Event,
			JobID: entry.JobID,
		}
		if len(bytes.TrimSpace(entry.Payload)) == 0 {
			itemResult.Error = "dead-letter payload is empty"
			result.Failed++
			result.Results = append(result.Results, itemResult)
			continue
		}

		sendResult := s.sendWebhookWithRetryAndContext(ctx, entry.Event, entry.JobID, entry.Payload, false)
		itemResult.Success = sendResult.Success
		itemResult.Attempts = sendResult.Attempts
		itemResult.LastStatusCode = sendResult.LastStatusCode
		itemResult.Error = sendResult.LastError
		result.Attempted += sendResult.Attempts
		if sendResult.Success {
			result.Succeeded++
		} else {
			result.Failed++
		}
		result.Results = append(result.Results, itemResult)
	}
	if result.Attempted > 0 {
		s.persist()
	}
	return result, nil
}

func (s *Service) GetJobEventsPage(jobID string, limit int, before int) ([]JobEvent, int, int, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > maxJobEvents {
		limit = maxJobEvents
	}

	s.mu.RLock()
	job, ok := s.jobs[jobID]
	if !ok {
		s.mu.RUnlock()
		return nil, 0, 0, errors.New("job not found")
	}
	events := make([]JobEvent, len(job.Events))
	copy(events, job.Events)
	s.mu.RUnlock()

	total := len(events)
	end := total
	if before > 0 && before < end {
		end = before
	}
	if before < 0 {
		before = 0
	}
	start := 0
	if end > limit {
		start = end - limit
	}
	page := make([]JobEvent, end-start)
	copy(page, events[start:end])
	nextBefore := start
	if nextBefore < 0 {
		nextBefore = 0
	}
	return page, total, nextBefore, nil
}

func (s *Service) ListJobs(limit int) []Job {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	s.mu.RLock()
	jobs := make([]Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, cloneJob(job))
	}
	s.mu.RUnlock()

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})
	if len(jobs) > limit {
		jobs = jobs[:limit]
	}
	return jobs
}

func (s *Service) RetryJob(jobID string) (Job, error) {
	s.mu.Lock()
	job, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return Job{}, errors.New("job not found")
	}
	if job.Status == "running" {
		s.mu.Unlock()
		return Job{}, errors.New("job is already running")
	}

	retried := 0
	for i := range job.Items {
		if job.Items[i].Status == "failed" {
			job.Items[i].Status = "queued"
			job.Items[i].Error = ""
			job.Items[i].OutputFilename = ""
			job.Items[i].OutputMime = ""
			job.Items[i].SizeBytes = 0
			job.Items[i].OutputBase64 = ""
			retried++
			s.appendJobEventLocked(job, "item.queued", "item re-queued by retry", i+1, "queued")
		}
	}
	if retried == 0 {
		s.mu.Unlock()
		return Job{}, errors.New("no failed items to retry")
	}

	job.Status = "queued"
	job.UpdatedAt = s.nowFn()
	job.FinishedAt = time.Time{}
	recomputeJobCounters(job)
	s.appendJobEventLocked(job, "job.retry_requested", "job queued for retry", 0, "queued")
	cloned := cloneJob(job)
	s.mu.Unlock()
	s.persist()

	s.enqueue(jobID)
	return cloned, nil
}

func (s *Service) CancelJob(jobID string) (Job, error) {
	s.mu.Lock()
	job, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return Job{}, errors.New("job not found")
	}

	switch job.Status {
	case "completed", "failed", "partial_failed":
		s.mu.Unlock()
		return Job{}, errors.New("job is already finished")
	case "canceled":
		cloned := cloneJob(job)
		s.mu.Unlock()
		return cloned, nil
	}

	wasRunning := false
	if cancelFn, exists := s.jobCancels[jobID]; exists {
		wasRunning = true
		cancelFn()
	}

	now := s.nowFn()
	job.CancelRequested = true
	job.Status = "canceled"
	job.UpdatedAt = now
	job.FinishedAt = now

	for i := range job.Items {
		if job.Items[i].Status == "queued" || job.Items[i].Status == "running" {
			job.Items[i].Status = "canceled"
			job.Items[i].Error = ""
			job.Items[i].OutputFilename = ""
			job.Items[i].OutputMime = ""
			job.Items[i].SizeBytes = 0
			job.Items[i].OutputBase64 = ""
			s.appendJobEventLocked(job, "item.canceled", "item canceled", i+1, "canceled")
		}
	}
	recomputeJobCounters(job)
	s.appendJobEventLocked(job, "job.canceled", "job canceled", 0, "canceled")
	cloned := cloneJob(job)
	s.mu.Unlock()

	s.persist()
	if !wasRunning {
		s.dispatchJobWebhook(cloned)
	}
	return cloned, nil
}

func (s *Service) ResumeJob(jobID string) (Job, error) {
	s.mu.Lock()
	job, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return Job{}, errors.New("job not found")
	}
	if job.Status != "canceled" {
		s.mu.Unlock()
		return Job{}, errors.New("job is not canceled")
	}
	if _, running := s.jobCancels[jobID]; running {
		s.mu.Unlock()
		return Job{}, errors.New("job cancellation is still in progress")
	}

	resumed := 0
	for i := range job.Items {
		if job.Items[i].Status == "canceled" {
			job.Items[i].Status = "queued"
			job.Items[i].Error = ""
			job.Items[i].OutputFilename = ""
			job.Items[i].OutputMime = ""
			job.Items[i].SizeBytes = 0
			job.Items[i].OutputBase64 = ""
			resumed++
			s.appendJobEventLocked(job, "item.queued", "item re-queued by resume", i+1, "queued")
		}
	}
	if resumed == 0 {
		s.mu.Unlock()
		return Job{}, errors.New("no canceled items to resume")
	}

	now := s.nowFn()
	job.CancelRequested = false
	job.Status = "queued"
	job.UpdatedAt = now
	job.FinishedAt = time.Time{}
	recomputeJobCounters(job)
	s.appendJobEventLocked(job, "job.resumed", "job resumed", 0, "queued")
	cloned := cloneJob(job)
	s.mu.Unlock()

	s.persist()
	s.enqueue(jobID)
	return cloned, nil
}

func (s *Service) ReadItemOutput(jobID string, itemIndex int) (ItemOutput, error) {
	if itemIndex < 0 {
		return ItemOutput{}, errors.New("item index must be >= 0")
	}

	s.mu.RLock()
	job, ok := s.jobs[jobID]
	if !ok {
		s.mu.RUnlock()
		return ItemOutput{}, errors.New("job not found")
	}
	if itemIndex >= len(job.Items) {
		s.mu.RUnlock()
		return ItemOutput{}, errors.New("item not found")
	}
	item := job.Items[itemIndex]
	s.mu.RUnlock()

	if item.Status != "completed" {
		return ItemOutput{}, errors.New("item output is not available")
	}
	if item.OutputFilename == "" {
		return ItemOutput{}, errors.New("item output is not available")
	}

	if item.OutputFilename != filepath.Base(item.OutputFilename) {
		return ItemOutput{}, errors.New("invalid output filename")
	}

	fullPath := filepath.Join(s.outputDir, item.OutputFilename)
	payload, err := os.ReadFile(fullPath)
	if err != nil {
		return ItemOutput{}, fmt.Errorf("failed reading output file: %w", err)
	}

	mime := strings.TrimSpace(item.OutputMime)
	if mime == "" {
		mime = "application/octet-stream"
	}
	return ItemOutput{
		Filename: item.OutputFilename,
		Mime:     mime,
		Data:     payload,
	}, nil
}

func (s *Service) enqueue(jobID string) {
	select {
	case s.queue <- jobID:
	default:
		go func() {
			select {
			case s.queue <- jobID:
			case <-s.ctx.Done():
			}
		}()
	}
}

func (s *Service) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case jobID := <-s.queue:
			s.processJob(jobID)
		}
	}
}

func (s *Service) processJob(jobID string) {
	s.mu.Lock()
	job, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return
	}
	switch job.Status {
	case "running", "completed", "failed", "partial_failed", "canceled":
		s.mu.Unlock()
		return
	}

	now := s.nowFn()
	job.Status = "running"
	job.UpdatedAt = now
	if job.StartedAt.IsZero() {
		job.StartedAt = now
	}
	s.appendJobEventLocked(job, "job.running", "job processing started", 0, "running")
	jobCtx, jobCancel := context.WithCancel(s.ctx)
	s.jobCancels[jobID] = jobCancel
	s.mu.Unlock()
	defer func() {
		jobCancel()
		s.mu.Lock()
		delete(s.jobCancels, jobID)
		s.mu.Unlock()
	}()
	s.persist()

	for idx := range job.Items {
		s.processItem(jobID, idx, jobCtx)
	}

	s.mu.Lock()
	job, ok = s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return
	}
	recomputeJobCounters(job)
	now = s.nowFn()
	if job.Status == "canceled" || job.CancelRequested {
		job.Status = "canceled"
		if job.FinishedAt.IsZero() {
			job.FinishedAt = now
		}
		job.UpdatedAt = now
		if !s.hasLastEventTypeLocked(job, "job.canceled") {
			s.appendJobEventLocked(job, "job.canceled", "job canceled", 0, "canceled")
		}
	} else {
		job.FinishedAt = now
		job.UpdatedAt = job.FinishedAt
		switch {
		case job.FailedItems == 0 && job.CompletedItems == job.TotalItems:
			job.Status = "completed"
		case job.CompletedItems == 0 && job.FailedItems == job.TotalItems:
			job.Status = "failed"
		default:
			job.Status = "partial_failed"
		}
		s.appendJobEventLocked(job, "job."+job.Status, "job processing finished", 0, job.Status)
	}
	finalJob := cloneJob(job)
	s.mu.Unlock()
	s.persist()
	s.dispatchJobWebhook(finalJob)
}

func (s *Service) processItem(jobID string, idx int, jobCtx context.Context) {
	s.mu.Lock()
	job, ok := s.jobs[jobID]
	if !ok || idx < 0 || idx >= len(job.Items) {
		s.mu.Unlock()
		return
	}
	if job.CancelRequested || job.Status == "canceled" {
		s.mu.Unlock()
		return
	}
	item := &job.Items[idx]
	if item.Status != "queued" {
		s.mu.Unlock()
		return
	}
	item.Status = "running"
	item.AttemptCount++
	item.LastAttemptedAt = s.nowFn()
	job.UpdatedAt = item.LastAttemptedAt
	s.appendJobEventLocked(job, "item.running", "item processing started", idx+1, "running")
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(jobCtx, 6*time.Minute)
	defer cancel()

	track, err := s.Lookup(ctx, LookupInput{URL: item.SourceURL})
	if err != nil {
		s.finishItem(jobID, idx, track, "", nil, err)
		return
	}

	filenameBase := normalizeFilename(track.Title)
	if filenameBase == "" {
		filenameBase = track.TrackID
	}
	outputFile := fmt.Sprintf("%s_%02d_%s.flac", jobID, idx+1, filenameBase)
	outputPath := filepath.Join(s.outputDir, outputFile)

	if err := s.runDownloader(ctx, track, outputPath); err != nil {
		s.finishItem(jobID, idx, track, "", nil, err)
		return
	}

	payload, err := os.ReadFile(outputPath)
	if err != nil {
		s.finishItem(jobID, idx, track, "", nil, fmt.Errorf("failed reading output: %w", err))
		return
	}
	s.finishItem(jobID, idx, track, outputFile, payload, nil)
}

func (s *Service) finishItem(jobID string, idx int, track TrackInfo, outputFile string, payload []byte, processErr error) {
	s.mu.Lock()
	job, ok := s.jobs[jobID]
	if !ok || idx < 0 || idx >= len(job.Items) {
		s.mu.Unlock()
		return
	}
	item := &job.Items[idx]
	if item.Status == "canceled" || job.Status == "canceled" || job.CancelRequested {
		job.UpdatedAt = s.nowFn()
		recomputeJobCounters(job)
		s.mu.Unlock()
		s.persist()
		return
	}

	item.Track = track
	if processErr != nil {
		item.Status = "failed"
		item.Error = processErr.Error()
		item.OutputFilename = ""
		item.OutputMime = ""
		item.SizeBytes = 0
		item.OutputBase64 = ""
	} else {
		item.Status = "completed"
		item.Error = ""
		item.OutputFilename = outputFile
		item.OutputMime = "audio/flac"
		item.SizeBytes = len(payload)
		if job.IncludeOutputBase64 {
			item.OutputBase64 = base64.StdEncoding.EncodeToString(payload)
		} else {
			item.OutputBase64 = ""
		}
	}
	s.appendJobEventLocked(job, "item."+item.Status, "item processing finished", idx+1, item.Status)
	job.UpdatedAt = s.nowFn()
	recomputeJobCounters(job)
	s.mu.Unlock()
	s.persist()
}

func (s *Service) runDownloader(ctx context.Context, track TrackInfo, outputPath string) error {
	if s.downloaderCmd == "" {
		return errors.New("downloader command is not configured")
	}

	cmd := exec.CommandContext(ctx, "sh", "-c", s.downloaderCmd)
	cmd.Env = append(os.Environ(),
		"SPOTIFY_SOURCE_URL="+track.SourceURL,
		"SPOTIFY_KIND="+track.Kind,
		"SPOTIFY_TRACK_ID="+track.TrackID,
		"SPOTIFY_TITLE="+track.Title,
		"SPOTIFY_ARTIST="+track.Artist,
		"SPOTIFY_ALBUM="+track.Album,
		"SPOTIFY_OUTPUT_PATH="+outputPath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(out))
		if message == "" {
			message = err.Error()
		}
		return fmt.Errorf("downloader command failed: %s", message)
	}
	return nil
}

func (s *Service) enrichFromOEmbed(ctx context.Context, track *TrackInfo) {
	oembedURL := s.lookupBaseURL + "/oembed?url=" + url.QueryEscape(track.SourceURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, oembedURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "apitera-spotify/1.0")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return
	}

	var payload struct {
		Title        string `json:"title"`
		ThumbnailURL string `json:"thumbnail_url"`
		Type         string `json:"type"`
		AuthorName   string `json:"author_name"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return
	}

	if payload.Title != "" {
		track.Title = strings.TrimSpace(payload.Title)
		artist, title := splitArtistAndTitle(payload.Title)
		if track.Artist == "" && artist != "" {
			track.Artist = artist
		}
		if title != "" {
			track.Title = title
		}
	}
	if track.Artist == "" && payload.AuthorName != "" {
		track.Artist = strings.TrimSpace(payload.AuthorName)
	}
	if payload.ThumbnailURL != "" {
		track.CoverURL = strings.TrimSpace(payload.ThumbnailURL)
	}
	if track.Kind == "album" && track.Album == "" {
		track.Album = track.Title
	}
}

func parseSpotifyReference(raw string) (spotifyRef, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return spotifyRef{}, errors.New("url is required")
	}

	if match := reSpotifyWeb.FindStringSubmatch(value); len(match) == 3 {
		kind := strings.ToLower(match[1])
		id := match[2]
		return spotifyRef{
			Kind:      kind,
			ID:        id,
			Canonical: "https://open.spotify.com/" + kind + "/" + id,
		}, nil
	}
	if match := reSpotifyURI.FindStringSubmatch(value); len(match) == 3 {
		kind := strings.ToLower(match[1])
		id := match[2]
		return spotifyRef{
			Kind:      kind,
			ID:        id,
			Canonical: "https://open.spotify.com/" + kind + "/" + id,
		}, nil
	}

	return spotifyRef{}, errors.New("unsupported spotify url format")
}

func recomputeJobCounters(job *Job) {
	completed := 0
	failed := 0
	for i := range job.Items {
		switch job.Items[i].Status {
		case "completed":
			completed++
		case "failed":
			failed++
		}
	}
	job.CompletedItems = completed
	job.FailedItems = failed
}

func cloneJob(job *Job) Job {
	cloned := *job
	cloned.Items = make([]JobItem, len(job.Items))
	copy(cloned.Items, job.Items)
	cloned.Events = make([]JobEvent, len(job.Events))
	copy(cloned.Events, job.Events)
	return cloned
}

func (s *Service) appendJobEventLocked(job *Job, eventType string, message string, itemIndex int, itemStatus string) {
	job.Events = append(job.Events, JobEvent{
		Timestamp:  s.nowFn(),
		Type:       eventType,
		Message:    strings.TrimSpace(message),
		ItemIndex:  itemIndex,
		ItemStatus: strings.TrimSpace(itemStatus),
	})
	if len(job.Events) > maxJobEvents {
		job.Events = append([]JobEvent(nil), job.Events[len(job.Events)-maxJobEvents:]...)
	}
}

func (s *Service) hasLastEventTypeLocked(job *Job, eventType string) bool {
	if len(job.Events) == 0 {
		return false
	}
	return job.Events[len(job.Events)-1].Type == eventType
}

func (s *Service) markWebhookAttempt(event string, jobID string, statusCode int, err error) {
	now := s.nowFn()
	s.webhookStatsMu.Lock()
	defer s.webhookStatsMu.Unlock()

	s.webhookStats.Attempts++
	s.webhookStats.LastEvent = event
	s.webhookStats.LastJobID = jobID
	s.webhookStats.LastStatusCode = statusCode
	s.webhookStats.UpdatedAt = now
	if err == nil {
		s.webhookStats.Successes++
		s.webhookStats.LastError = ""
		return
	}
	s.webhookStats.Failures++
	s.webhookStats.LastError = err.Error()
}

func (s *Service) markWebhookRetried() {
	s.webhookStatsMu.Lock()
	s.webhookStats.RetriedEvents++
	s.webhookStats.UpdatedAt = s.nowFn()
	s.webhookStatsMu.Unlock()
}

func (s *Service) markWebhookDeadLetter(entry WebhookFailure) {
	s.webhookStatsMu.Lock()
	s.webhookStats.DeadLetters++
	s.webhookStats.LastEvent = entry.Event
	s.webhookStats.LastJobID = entry.JobID
	s.webhookStats.LastStatusCode = entry.LastStatusCode
	s.webhookStats.LastError = entry.Error
	s.webhookStats.UpdatedAt = s.nowFn()
	s.webhookStatsMu.Unlock()
}

func (s *Service) dispatchJobWebhook(job Job) {
	if strings.TrimSpace(s.webhookURL) == "" {
		return
	}

	event := "spotify.job." + job.Status
	payload := map[string]any{
		"event":           event,
		"job_id":          job.ID,
		"status":          job.Status,
		"created_at":      job.CreatedAt,
		"started_at":      job.StartedAt,
		"finished_at":     job.FinishedAt,
		"updated_at":      job.UpdatedAt,
		"total_items":     job.TotalItems,
		"completed_items": job.CompletedItems,
		"failed_items":    job.FailedItems,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	go s.sendWebhookWithRetry(event, job.ID, data)
}

func signWebhookPayload(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

type webhookSendResult struct {
	Attempts       int
	Success        bool
	LastStatusCode int
	LastError      string
}

func (s *Service) sendWebhookWithRetry(event string, jobID string, body []byte) {
	_ = s.sendWebhookWithRetryAndContext(s.ctx, event, jobID, body, true)
	s.persist()
}

func (s *Service) sendWebhookWithRetryAndContext(ctx context.Context, event string, jobID string, body []byte, writeDeadLetter bool) webhookSendResult {
	maxAttempts := s.webhookRetries
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	result := webhookSendResult{}
	retriedEvent := false
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if s.isWebhookDeliveryCancelled(ctx) {
			if result.LastError == "" {
				result.LastError = "webhook delivery canceled"
			}
			return result
		}
		if attempt > 1 && !retriedEvent {
			retriedEvent = true
			s.markWebhookRetried()
		}
		statusCode, err := s.sendWebhookAttempt(ctx, event, jobID, body)
		result.Attempts++
		result.LastStatusCode = statusCode
		s.markWebhookAttempt(event, jobID, statusCode, err)
		if err == nil {
			result.Success = true
			result.LastError = ""
			return result
		}
		result.LastError = err.Error()

		if attempt == maxAttempts {
			break
		}
		backoff := s.webhookRetryBackoff(attempt)
		if !s.waitWebhookRetryBackoff(ctx, backoff) {
			if result.LastError == "" {
				result.LastError = "webhook retry interrupted"
			}
			return result
		}
	}

	if writeDeadLetter {
		s.appendWebhookFailure(WebhookFailure{
			Timestamp:      s.nowFn(),
			Event:          event,
			JobID:          jobID,
			URL:            s.webhookURL,
			Attempts:       maxAttempts,
			LastStatusCode: result.LastStatusCode,
			Error:          result.LastError,
			Payload:        json.RawMessage(body),
		})
	}
	return result
}

func (s *Service) sendWebhookAttempt(parent context.Context, event string, jobID string, body []byte) (int, error) {
	if parent == nil {
		parent = s.ctx
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "apitera-spotify/1.0")
	req.Header.Set("X-Spotify-Event", event)
	req.Header.Set("X-Spotify-Job-ID", jobID)
	if s.webhookSecret != "" {
		req.Header.Set("X-Spotify-Signature", signWebhookPayload(body, s.webhookSecret))
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.StatusCode, nil
	}
	return resp.StatusCode, fmt.Errorf("webhook returned status %d", resp.StatusCode)
}

func (s *Service) isWebhookDeliveryCancelled(ctx context.Context) bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return true
		default:
		}
	}
	return false
}

func (s *Service) webhookRetryBackoff(attempt int) time.Duration {
	backoff := s.webhookDelay
	if backoff <= 0 {
		backoff = 400 * time.Millisecond
	}
	if attempt > 1 {
		factor := time.Duration(1 << uint(attempt-1))
		if factor > 32 {
			factor = 32
		}
		backoff *= factor
	}
	return backoff
}

func (s *Service) waitWebhookRetryBackoff(ctx context.Context, backoff time.Duration) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	if ctx == nil {
		select {
		case <-s.ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}

	select {
	case <-s.ctx.Done():
		return false
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Service) appendWebhookFailure(entry WebhookFailure) {
	payload, err := json.Marshal(entry)
	if err != nil {
		return
	}

	s.webhookMu.Lock()
	defer s.webhookMu.Unlock()

	file, err := os.OpenFile(s.webhookDLQFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer file.Close()

	_, _ = file.Write(payload)
	_, _ = file.Write([]byte("\n"))
	s.markWebhookDeadLetter(entry)
}

func (s *Service) nextJobID() string {
	idx := atomic.AddUint64(&s.counter, 1)
	return fmt.Sprintf("job-%d-%d", s.nowFn().Unix(), idx)
}

func normalizeFilename(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		isAlphaNum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlphaNum {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteRune('-')
			lastDash = true
		}
	}
	cleaned := strings.Trim(b.String(), "-")
	if len(cleaned) > 80 {
		cleaned = strings.Trim(cleaned[:80], "-")
	}
	return cleaned
}

func splitArtistAndTitle(value string) (artist string, title string) {
	parts := strings.SplitN(value, " - ", 2)
	if len(parts) == 2 {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return "", strings.TrimSpace(value)
}

func (s *Service) loadState() []string {
	raw, err := os.ReadFile(s.stateFile)
	if err != nil {
		return nil
	}

	var state persistedState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil
	}

	queued := make([]string, 0, len(state.Jobs))
	now := s.nowFn()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.counter = state.Counter
	for i := range state.Jobs {
		job := state.Jobs[i]
		for idx := range job.Items {
			if job.Items[idx].Status == "running" {
				job.Items[idx].Status = "queued"
				job.Items[idx].Error = ""
			}
		}

		if job.Status == "running" || job.Status == "queued" {
			job.Status = "queued"
			job.UpdatedAt = now
			job.FinishedAt = time.Time{}
			for idx := range job.Items {
				if job.Items[idx].Status == "running" || job.Items[idx].Status == "queued" {
					job.Items[idx].Status = "queued"
					job.Items[idx].Error = ""
				}
			}
		}

		recomputeJobCounters(&job)
		cloned := job
		s.jobs[job.ID] = &cloned
		if cloned.Status == "queued" {
			queued = append(queued, cloned.ID)
		}
	}
	return queued
}

func (s *Service) persist() {
	s.mu.RLock()
	state := persistedState{
		Counter: s.counter,
		Jobs:    make([]Job, 0, len(s.jobs)),
	}
	for _, job := range s.jobs {
		state.Jobs = append(state.Jobs, cloneJob(job))
	}
	s.mu.RUnlock()

	sort.Slice(state.Jobs, func(i, j int) bool {
		return state.Jobs[i].CreatedAt.Before(state.Jobs[j].CreatedAt)
	})

	payload, err := json.Marshal(state)
	if err != nil {
		return
	}

	tmpFile := s.stateFile + ".tmp"
	if err := os.WriteFile(tmpFile, payload, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmpFile, s.stateFile)
}
