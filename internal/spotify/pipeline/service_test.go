package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLookup(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oembed" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"title":"Daft Punk - Harder Better Faster Stronger","thumbnail_url":"https://cdn.example.com/cover.jpg","author_name":"Daft Punk"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{LookupBaseURL: oembedServer.URL})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	result, err := svc.Lookup(context.Background(), LookupInput{URL: "https://open.spotify.com/track/7ouMYWpwJ422jRcDASZB7P"})
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if result.Kind != "track" || result.TrackID != "7ouMYWpwJ422jRcDASZB7P" {
		t.Fatalf("unexpected kind/id: %s %s", result.Kind, result.TrackID)
	}
	if result.Artist != "Daft Punk" {
		t.Fatalf("unexpected artist: %s", result.Artist)
	}
	if result.Title != "Harder Better Faster Stronger" {
		t.Fatalf("unexpected title: %s", result.Title)
	}
}

func TestCreateJobAndProcess(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song","thumbnail_url":"https://cdn.example.com/cover.jpg"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd: "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{
			"https://open.spotify.com/track/aaaaaaaaaaaaaaa",
			"spotify:album:bbbbbbbbbbbbbbb",
		},
		IncludeOutputBase64: true,
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	final := waitForJobStatus(t, svc, job.ID, 5*time.Second)
	if final.Status != "completed" {
		t.Fatalf("expected completed job, got %s", final.Status)
	}
	if final.CompletedItems != 2 || final.FailedItems != 0 {
		t.Fatalf("unexpected counters: completed=%d failed=%d", final.CompletedItems, final.FailedItems)
	}
	if final.Items[0].OutputMime != "audio/flac" || final.Items[0].OutputBase64 == "" {
		t.Fatalf("expected output mime and base64")
	}
}

func TestRetryFailedItems(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd: "if [ \"$SPOTIFY_TRACK_ID\" = \"badidbadid1\" ]; then exit 1; fi; printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{
			"spotify:track:goodidgoodid1",
			"spotify:track:badidbadid1",
		},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	first := waitForJobStatus(t, svc, job.ID, 5*time.Second)
	if first.Status != "partial_failed" {
		t.Fatalf("expected partial_failed, got %s", first.Status)
	}
	if first.FailedItems != 1 {
		t.Fatalf("expected 1 failed item, got %d", first.FailedItems)
	}

	svc.downloaderCmd = "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\""
	if _, err := svc.RetryJob(job.ID); err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	retried := waitForJobStatus(t, svc, job.ID, 5*time.Second)
	if retried.Status != "completed" {
		t.Fatalf("expected completed after retry, got %s", retried.Status)
	}
}

func TestJobWebhookDeliveryAndSignature(t *testing.T) {
	type webhookCall struct {
		Body  []byte
		Sig   string
		Event string
		JobID string
	}
	webhookCalls := make(chan webhookCall, 2)

	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/webhook" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, _ := io.ReadAll(r.Body)
		webhookCalls <- webhookCall{
			Body:  body,
			Sig:   r.Header.Get("X-Spotify-Signature"),
			Event: r.Header.Get("X-Spotify-Event"),
			JobID: r.Header.Get("X-Spotify-Job-ID"),
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer webhookServer.Close()

	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd: "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
		WebhookURL:    webhookServer.URL + "/webhook",
		WebhookSecret: "test-secret",
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{"spotify:track:webhook001"},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	done := waitForJobStatus(t, svc, job.ID, 5*time.Second)
	if done.Status != "completed" {
		t.Fatalf("expected completed status, got %s", done.Status)
	}

	select {
	case call := <-webhookCalls:
		if call.Event != "spotify.job.completed" {
			t.Fatalf("unexpected event header: %s", call.Event)
		}
		if call.JobID != job.ID {
			t.Fatalf("unexpected job id header: %s", call.JobID)
		}
		expectedSig := signWebhookPayload(call.Body, "test-secret")
		if call.Sig != expectedSig {
			t.Fatalf("unexpected signature: got=%s want=%s", call.Sig, expectedSig)
		}

		var payload struct {
			Event          string `json:"event"`
			JobID          string `json:"job_id"`
			Status         string `json:"status"`
			TotalItems     int    `json:"total_items"`
			CompletedItems int    `json:"completed_items"`
			FailedItems    int    `json:"failed_items"`
		}
		if err := json.Unmarshal(call.Body, &payload); err != nil {
			t.Fatalf("unmarshal webhook payload: %v", err)
		}
		if payload.Event != "spotify.job.completed" || payload.JobID != job.ID || payload.Status != "completed" {
			t.Fatalf("unexpected webhook payload: %+v", payload)
		}
		if payload.TotalItems != 1 || payload.CompletedItems != 1 || payload.FailedItems != 0 {
			t.Fatalf("unexpected webhook counters: %+v", payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for webhook")
	}
}

func TestJobWebhookRetriesThenSucceeds(t *testing.T) {
	attempts := 0
	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/webhook" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer webhookServer.Close()

	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd:  "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:      t.TempDir(),
		WorkerCount:    1,
		LookupBaseURL:  oembedServer.URL,
		WebhookURL:     webhookServer.URL + "/webhook",
		WebhookRetries: 5,
		WebhookRetryMS: 20,
		WebhookSecret:  "retry-secret",
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{"spotify:track:webhookretry001"},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	done := waitForJobStatus(t, svc, job.ID, 5*time.Second)
	if done.Status != "completed" {
		t.Fatalf("expected completed status, got %s", done.Status)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if attempts >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 webhook attempts, got %d", attempts)
	}

	if _, err := os.Stat(svc.webhookDLQFile); err == nil {
		t.Fatalf("did not expect DLQ file for eventually successful webhook")
	}
}

func TestJobWebhookFailureWritesDeadLetter(t *testing.T) {
	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/webhook" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer webhookServer.Close()

	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd:  "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:      t.TempDir(),
		WorkerCount:    1,
		LookupBaseURL:  oembedServer.URL,
		WebhookURL:     webhookServer.URL + "/webhook",
		WebhookRetries: 2,
		WebhookRetryMS: 20,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{"spotify:track:webhookdlq001"},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	done := waitForJobStatus(t, svc, job.ID, 5*time.Second)
	if done.Status != "completed" {
		t.Fatalf("expected completed status, got %s", done.Status)
	}

	deadline := time.Now().Add(3 * time.Second)
	var raw []byte
	for time.Now().Before(deadline) {
		raw, err = os.ReadFile(svc.webhookDLQFile)
		if err == nil && len(bytes.TrimSpace(raw)) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		t.Fatalf("expected webhook dead-letter entry")
	}

	lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
	last := lines[len(lines)-1]
	var entry struct {
		Event          string `json:"event"`
		JobID          string `json:"job_id"`
		Attempts       int    `json:"attempts"`
		LastStatusCode int    `json:"last_status_code"`
		Error          string `json:"error"`
	}
	if err := json.Unmarshal(last, &entry); err != nil {
		t.Fatalf("unmarshal dead-letter: %v", err)
	}
	if entry.Event != "spotify.job.completed" || entry.JobID != job.ID {
		t.Fatalf("unexpected dead-letter entry: %+v", entry)
	}
	if entry.Attempts != 2 || entry.LastStatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected dead-letter attempts/status: %+v", entry)
	}
	if entry.Error == "" {
		t.Fatalf("expected dead-letter error detail")
	}

	metrics := svc.GetWebhookMetrics()
	if metrics.Attempts < 2 || metrics.Failures < 2 {
		t.Fatalf("unexpected webhook metrics: %+v", metrics)
	}
	if metrics.DeadLetters < 1 {
		t.Fatalf("expected dead letters >= 1, got %+v", metrics)
	}
	failures, err := svc.ListWebhookFailures(5)
	if err != nil {
		t.Fatalf("list webhook failures: %v", err)
	}
	if len(failures) < 1 {
		t.Fatalf("expected failures list to include dead-letter entries")
	}
}

func TestGetWebhookFailuresPageFiltersAndCursor(t *testing.T) {
	svc, err := NewService(Config{
		OutputDir:   t.TempDir(),
		WorkerCount: 1,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	now := time.Now().UTC()
	svc.appendWebhookFailure(WebhookFailure{
		Timestamp:      now.Add(1 * time.Second),
		Event:          "spotify.job.completed",
		JobID:          "job-a",
		URL:            "http://example.test/webhook",
		Attempts:       2,
		LastStatusCode: http.StatusBadGateway,
		Error:          "bad gateway",
	})
	svc.appendWebhookFailure(WebhookFailure{
		Timestamp:      now.Add(2 * time.Second),
		Event:          "spotify.job.failed",
		JobID:          "job-b",
		URL:            "http://example.test/webhook",
		Attempts:       2,
		LastStatusCode: http.StatusBadGateway,
		Error:          "bad gateway",
	})
	svc.appendWebhookFailure(WebhookFailure{
		Timestamp:      now.Add(3 * time.Second),
		Event:          "spotify.job.completed",
		JobID:          "job-a",
		URL:            "http://example.test/webhook",
		Attempts:       2,
		LastStatusCode: http.StatusBadGateway,
		Error:          "bad gateway",
	})

	page1, total1, next1, err := svc.GetWebhookFailuresPage(1, 0, "job-a", "")
	if err != nil {
		t.Fatalf("get webhook failures page1: %v", err)
	}
	if total1 != 2 || len(page1) != 1 || next1 != 1 {
		t.Fatalf("unexpected page1 values: total=%d len=%d next=%d", total1, len(page1), next1)
	}
	if page1[0].JobID != "job-a" {
		t.Fatalf("expected filtered job-a, got %s", page1[0].JobID)
	}

	page2, total2, next2, err := svc.GetWebhookFailuresPage(1, next1, "job-a", "")
	if err != nil {
		t.Fatalf("get webhook failures page2: %v", err)
	}
	if total2 != 2 || len(page2) != 1 || next2 != 0 {
		t.Fatalf("unexpected page2 values: total=%d len=%d next=%d", total2, len(page2), next2)
	}
	if page2[0].JobID != "job-a" {
		t.Fatalf("expected filtered job-a on page2, got %s", page2[0].JobID)
	}

	eventPage, totalEvent, _, err := svc.GetWebhookFailuresPage(10, 0, "", "spotify.job.failed")
	if err != nil {
		t.Fatalf("get webhook failures event filter: %v", err)
	}
	if totalEvent != 1 || len(eventPage) != 1 {
		t.Fatalf("unexpected event-filter values: total=%d len=%d", totalEvent, len(eventPage))
	}
	if eventPage[0].Event != "spotify.job.failed" || eventPage[0].JobID != "job-b" {
		t.Fatalf("unexpected event-filter entry: %+v", eventPage[0])
	}
}

func TestReplayWebhookFailures(t *testing.T) {
	attempts := 0
	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/webhook" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		attempts++
		if attempts <= 2 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer webhookServer.Close()

	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd:  "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:      t.TempDir(),
		WorkerCount:    1,
		LookupBaseURL:  oembedServer.URL,
		WebhookURL:     webhookServer.URL + "/webhook",
		WebhookRetries: 2,
		WebhookRetryMS: 20,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{"spotify:track:replay001"},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	done := waitForJobStatus(t, svc, job.ID, 5*time.Second)
	if done.Status != "completed" {
		t.Fatalf("expected completed status, got %s", done.Status)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, _, _, ferr := svc.GetWebhookFailuresPage(10, 0, job.ID, "spotify.job.completed")
		if ferr == nil && len(entries) >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	replayResult, err := svc.ReplayWebhookFailures(context.Background(), ReplayWebhookInput{
		Limit: 10,
		JobID: job.ID,
		Event: "spotify.job.completed",
	})
	if err != nil {
		t.Fatalf("replay webhook failures: %v", err)
	}
	if replayResult.Selected < 1 || replayResult.Succeeded < 1 || replayResult.Failed != 0 {
		t.Fatalf("unexpected replay result: %+v", replayResult)
	}
	if len(replayResult.Results) < 1 || !replayResult.Results[0].Success {
		t.Fatalf("expected first replay result to succeed: %+v", replayResult.Results)
	}

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if attempts >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if attempts < 3 {
		t.Fatalf("expected replay to produce another webhook attempt, got %d", attempts)
	}
}

func TestCancelJobStopsProcessing(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd: "sleep 5; printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{
			"spotify:track:canceljob001",
			"spotify:track:canceljob002",
		},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	canceled, err := svc.CancelJob(job.ID)
	if err != nil {
		t.Fatalf("cancel job failed: %v", err)
	}
	if canceled.Status != "canceled" {
		t.Fatalf("expected canceled status, got %s", canceled.Status)
	}
	if !canceled.CancelRequested {
		t.Fatalf("expected cancel_requested=true")
	}

	final := waitForJobStatus(t, svc, job.ID, 6*time.Second)
	if final.Status != "canceled" {
		t.Fatalf("expected final canceled status, got %s", final.Status)
	}
	for i := range final.Items {
		if final.Items[i].Status == "queued" || final.Items[i].Status == "running" {
			t.Fatalf("expected no queued/running items after cancel, got %s", final.Items[i].Status)
		}
	}
}

func TestCancelFinishedJobFails(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd: "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{"spotify:track:canceljob003"},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	done := waitForJobStatus(t, svc, job.ID, 5*time.Second)
	if done.Status != "completed" {
		t.Fatalf("expected completed status, got %s", done.Status)
	}

	if _, err := svc.CancelJob(job.ID); err == nil {
		t.Fatalf("expected cancel to fail for finished job")
	}
}

func TestResumeCanceledJob(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd: "sleep 2; printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{
			"spotify:track:resumejob001",
			"spotify:track:resumejob002",
		},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	canceled, err := svc.CancelJob(job.ID)
	if err != nil {
		t.Fatalf("cancel job failed: %v", err)
	}
	if canceled.Status != "canceled" {
		t.Fatalf("expected canceled status, got %s", canceled.Status)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		svc.mu.RLock()
		_, running := svc.jobCancels[job.ID]
		svc.mu.RUnlock()
		if !running {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	svc.downloaderCmd = "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\""
	resumed, err := svc.ResumeJob(job.ID)
	if err != nil {
		t.Fatalf("resume job failed: %v", err)
	}
	if resumed.Status != "queued" || resumed.CancelRequested {
		t.Fatalf("unexpected resumed state: status=%s cancel_requested=%v", resumed.Status, resumed.CancelRequested)
	}

	final := waitForJobStatus(t, svc, job.ID, 8*time.Second)
	if final.Status != "completed" {
		t.Fatalf("expected completed status after resume, got %s", final.Status)
	}
	if final.CompletedItems != 2 || final.FailedItems != 0 {
		t.Fatalf("unexpected counters after resume: completed=%d failed=%d", final.CompletedItems, final.FailedItems)
	}

	events, err := svc.GetJobEvents(job.ID, 200)
	if err != nil {
		t.Fatalf("get events failed: %v", err)
	}
	if !containsEventType(events, "job.resumed") {
		t.Fatalf("expected job.resumed event")
	}
	if !containsEventType(events, "job.completed") {
		t.Fatalf("expected job.completed event")
	}
}

func TestGetJobEventsLimit(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd: "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{"spotify:track:eventlimit001"},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	_ = waitForJobStatus(t, svc, job.ID, 5*time.Second)

	allEvents, err := svc.GetJobEvents(job.ID, 200)
	if err != nil {
		t.Fatalf("get all events failed: %v", err)
	}
	if len(allEvents) < 3 {
		t.Fatalf("expected multiple lifecycle events, got %d", len(allEvents))
	}
	if !containsEventType(allEvents, "job.created") || !containsEventType(allEvents, "job.completed") {
		t.Fatalf("expected created/completed events")
	}

	limited, err := svc.GetJobEvents(job.ID, 2)
	if err != nil {
		t.Fatalf("get limited events failed: %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("expected exactly 2 events in limited response, got %d", len(limited))
	}
}

func TestGetJobEventsPageCursor(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd: "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{"spotify:track:eventcursor001"},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	_ = waitForJobStatus(t, svc, job.ID, 5*time.Second)

	firstPage, total, nextBefore, err := svc.GetJobEventsPage(job.ID, 2, 0)
	if err != nil {
		t.Fatalf("get first page failed: %v", err)
	}
	if total < 3 {
		t.Fatalf("expected at least 3 events, got %d", total)
	}
	if len(firstPage) != 2 {
		t.Fatalf("expected first page size 2, got %d", len(firstPage))
	}
	if nextBefore <= 0 {
		t.Fatalf("expected next_before > 0, got %d", nextBefore)
	}

	secondPage, total2, nextBefore2, err := svc.GetJobEventsPage(job.ID, 2, nextBefore)
	if err != nil {
		t.Fatalf("get second page failed: %v", err)
	}
	if total2 != total {
		t.Fatalf("expected same total, got %d vs %d", total2, total)
	}
	if len(secondPage) == 0 {
		t.Fatalf("expected second page to contain older events")
	}
	if nextBefore2 < 0 || nextBefore2 >= nextBefore {
		t.Fatalf("unexpected next_before progression: %d -> %d", nextBefore, nextBefore2)
	}
	if firstPage[0].Timestamp.Before(secondPage[len(secondPage)-1].Timestamp) {
		t.Fatalf("expected first page to be newer than second page")
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	root := t.TempDir()
	cfg := Config{
		DownloaderCmd: "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     filepath.Join(root, "output"),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
		StateFile:     filepath.Join(root, "jobs_state.json"),
	}

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{"spotify:track:persisttrack01"},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	done := waitForJobStatus(t, svc, job.ID, 5*time.Second)
	if done.Status != "completed" {
		t.Fatalf("expected completed status before restart, got %s", done.Status)
	}
	svc.Close()

	svc2, err := NewService(cfg)
	if err != nil {
		t.Fatalf("new service after restart: %v", err)
	}
	defer svc2.Close()

	restored, err := svc2.GetJob(job.ID)
	if err != nil {
		t.Fatalf("expected restored job, got error: %v", err)
	}
	if restored.Status != "completed" || restored.CompletedItems != 1 {
		t.Fatalf("unexpected restored state: status=%s completed=%d", restored.Status, restored.CompletedItems)
	}
}

func TestResumeQueuedOrRunningJobsOnStartup(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	root := t.TempDir()
	stateFile := filepath.Join(root, "jobs_state.json")
	now := time.Now().UTC()

	state := persistedState{
		Counter: 7,
		Jobs: []Job{
			{
				ID:         "job-restore-1",
				Status:     "running",
				CreatedAt:  now.Add(-2 * time.Minute),
				UpdatedAt:  now.Add(-1 * time.Minute),
				StartedAt:  now.Add(-2 * time.Minute),
				TotalItems: 1,
				Items: []JobItem{
					{
						SourceURL: "spotify:track:resumeid0001",
						Status:    "running",
					},
				},
			},
		},
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	if err := os.WriteFile(stateFile, raw, 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}

	svc, err := NewService(Config{
		DownloaderCmd: "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     filepath.Join(root, "output"),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
		StateFile:     stateFile,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	resumed := waitForJobStatus(t, svc, "job-restore-1", 5*time.Second)
	if resumed.Status != "completed" {
		t.Fatalf("expected resumed job completion, got %s", resumed.Status)
	}
	if resumed.Items[0].Status != "completed" || resumed.Items[0].AttemptCount < 1 {
		t.Fatalf("expected resumed item processed, got status=%s attempts=%d", resumed.Items[0].Status, resumed.Items[0].AttemptCount)
	}
}

func TestReadItemOutput(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd: "printf 'fLaCsmoke' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{"spotify:track:readoutput001"},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	done := waitForJobStatus(t, svc, job.ID, 5*time.Second)
	if done.Status != "completed" {
		t.Fatalf("expected completed status, got %s", done.Status)
	}

	output, err := svc.ReadItemOutput(job.ID, 0)
	if err != nil {
		t.Fatalf("read item output: %v", err)
	}
	if output.Mime != "audio/flac" {
		t.Fatalf("unexpected mime: %s", output.Mime)
	}
	if len(output.Data) == 0 || string(output.Data) != "fLaCsmoke" {
		t.Fatalf("unexpected output payload: %q", string(output.Data))
	}
	if output.Filename == "" {
		t.Fatalf("expected output filename")
	}
}

func TestReadItemOutputNotAvailable(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := NewService(Config{
		DownloaderCmd: "exit 1",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), CreateJobInput{
		Items: []string{"spotify:track:readoutput002"},
	})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	_ = waitForJobStatus(t, svc, job.ID, 5*time.Second)

	if _, err := svc.ReadItemOutput(job.ID, 0); err == nil {
		t.Fatalf("expected output read to fail for failed item")
	}
}

func waitForJobStatus(t *testing.T, svc *Service, jobID string, timeout time.Duration) Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := svc.GetJob(jobID)
		if err != nil {
			t.Fatalf("get job failed: %v", err)
		}
		switch job.Status {
		case "completed", "failed", "partial_failed", "canceled":
			return job
		}
		time.Sleep(20 * time.Millisecond)
	}
	job, _ := svc.GetJob(jobID)
	t.Fatalf("timeout waiting for job completion: status=%s", job.Status)
	return Job{}
}

func containsEventType(events []JobEvent, eventType string) bool {
	for i := range events {
		if events[i].Type == eventType {
			return true
		}
	}
	return false
}
