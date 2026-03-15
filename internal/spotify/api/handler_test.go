package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"apiservices/spotify-flac/internal/spotify/pipeline"
)

func TestEventsEndpointPagination(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := pipeline.NewService(pipeline.Config{
		DownloaderCmd: "printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	job, err := svc.CreateJob(context.Background(), pipeline.CreateJobInput{
		Items: []string{"spotify:track:eventsapi001"},
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	waitForTerminalJobStatus(t, svc, job.ID, 5*time.Second)

	handler := NewHandler(svc)
	req := httptest.NewRequest(http.MethodGet, "/v1/spotify/jobs/"+job.ID+"/events?limit=2", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	var payload struct {
		Data struct {
			JobID      string              `json:"job_id"`
			Count      int                 `json:"count"`
			Total      int                 `json:"total"`
			Before     int                 `json:"before"`
			NextBefore int                 `json:"next_before"`
			Events     []pipeline.JobEvent `json:"events"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if payload.Data.JobID != job.ID {
		t.Fatalf("unexpected job id: %s", payload.Data.JobID)
	}
	if payload.Data.Total < 3 || payload.Data.Count != len(payload.Data.Events) || payload.Data.Count > 2 {
		t.Fatalf("unexpected paging payload: %+v", payload.Data)
	}
	if payload.Data.NextBefore <= 0 {
		t.Fatalf("expected next_before > 0, got %d", payload.Data.NextBefore)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/v1/spotify/jobs/"+job.ID+"/events?limit=2&before="+strconv.Itoa(payload.Data.NextBefore), nil)
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 on second page, got %d: %s", rr2.Code, rr2.Body.String())
	}

	req3 := httptest.NewRequest(http.MethodGet, "/v1/spotify/jobs/"+job.ID+"/events?before=bad", nil)
	rr3 := httptest.NewRecorder()
	handler.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad before, got %d", rr3.Code)
	}
}

func TestCancelAndResumeEndpoints(t *testing.T) {
	oembedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"title":"Artist - Song"}`))
	}))
	defer oembedServer.Close()

	svc, err := pipeline.NewService(pipeline.Config{
		DownloaderCmd: "if echo \"$SPOTIFY_TRACK_ID\" | grep -q '^cancel'; then sleep 1; fi; printf 'fLaC' > \"$SPOTIFY_OUTPUT_PATH\"",
		OutputDir:     t.TempDir(),
		WorkerCount:   1,
		LookupBaseURL: oembedServer.URL,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	handler := NewHandler(svc)

	createReq := httptest.NewRequest(http.MethodPost, "/v1/spotify/jobs", strings.NewReader(`{"items":["spotify:track:cancelapi001","spotify:track:cancelapi002"]}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRR := httptest.NewRecorder()
	handler.ServeHTTP(createRR, createReq)
	if createRR.Code != http.StatusCreated {
		t.Fatalf("expected 201 for create, got %d: %s", createRR.Code, createRR.Body.String())
	}
	var created struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(createRR.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}
	if created.Data.ID == "" {
		t.Fatalf("missing job id in create response")
	}

	time.Sleep(80 * time.Millisecond)
	cancelReq := httptest.NewRequest(http.MethodPost, "/v1/spotify/jobs/"+created.Data.ID+"/cancel", nil)
	cancelRR := httptest.NewRecorder()
	handler.ServeHTTP(cancelRR, cancelReq)
	if cancelRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for cancel, got %d: %s", cancelRR.Code, cancelRR.Body.String())
	}

	waitForStatusViaAPI(t, handler, created.Data.ID, "canceled", 5*time.Second)

	resumeOK := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resumeReq := httptest.NewRequest(http.MethodPost, "/v1/spotify/jobs/"+created.Data.ID+"/resume", nil)
		resumeRR := httptest.NewRecorder()
		handler.ServeHTTP(resumeRR, resumeReq)
		if resumeRR.Code == http.StatusOK {
			resumeOK = true
			break
		}
		time.Sleep(80 * time.Millisecond)
	}
	if !resumeOK {
		t.Fatalf("expected resume endpoint to eventually succeed")
	}

	waitForStatusViaAPI(t, handler, created.Data.ID, "completed", 8*time.Second)

	eventsReq := httptest.NewRequest(http.MethodGet, "/v1/spotify/jobs/"+created.Data.ID+"/events?limit=200", nil)
	eventsRR := httptest.NewRecorder()
	handler.ServeHTTP(eventsRR, eventsReq)
	if eventsRR.Code != http.StatusOK {
		t.Fatalf("expected 200 for events, got %d: %s", eventsRR.Code, eventsRR.Body.String())
	}
	if !strings.Contains(eventsRR.Body.String(), `"job.resumed"`) {
		t.Fatalf("expected events payload to contain job.resumed")
	}
}

func TestWebhookMetricsAndFailuresEndpoints(t *testing.T) {
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

	svc, err := pipeline.NewService(pipeline.Config{
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

	job, err := svc.CreateJob(context.Background(), pipeline.CreateJobInput{
		Items: []string{"spotify:track:webhookmetrics001"},
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	waitForTerminalJobStatus(t, svc, job.ID, 5*time.Second)

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		failures, ferr := svc.ListWebhookFailures(10)
		if ferr == nil && len(failures) >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	handler := NewHandler(svc)

	metricsReq := httptest.NewRequest(http.MethodGet, "/v1/spotify/webhooks/metrics", nil)
	metricsRR := httptest.NewRecorder()
	handler.ServeHTTP(metricsRR, metricsReq)
	if metricsRR.Code != http.StatusOK {
		t.Fatalf("expected metrics 200, got %d: %s", metricsRR.Code, metricsRR.Body.String())
	}
	if !strings.Contains(metricsRR.Body.String(), `"attempts":`) || !strings.Contains(metricsRR.Body.String(), `"dead_letters":`) {
		t.Fatalf("unexpected metrics payload: %s", metricsRR.Body.String())
	}

	failReq := httptest.NewRequest(http.MethodGet, "/v1/spotify/webhooks/failures?limit=5", nil)
	failRR := httptest.NewRecorder()
	handler.ServeHTTP(failRR, failReq)
	if failRR.Code != http.StatusOK {
		t.Fatalf("expected failures 200, got %d: %s", failRR.Code, failRR.Body.String())
	}
	var failPayload struct {
		Data struct {
			Total      int                       `json:"total"`
			Count      int                       `json:"count"`
			Before     int                       `json:"before"`
			NextBefore int                       `json:"next_before"`
			JobID      string                    `json:"job_id"`
			Event      string                    `json:"event"`
			Failures   []pipeline.WebhookFailure `json:"failures"`
		} `json:"data"`
	}
	if err := json.Unmarshal(failRR.Body.Bytes(), &failPayload); err != nil {
		t.Fatalf("unmarshal failures response: %v", err)
	}
	if failPayload.Data.Total < 1 || failPayload.Data.Count < 1 || len(failPayload.Data.Failures) < 1 {
		t.Fatalf("unexpected failures payload: %+v", failPayload.Data)
	}

	target := failPayload.Data.Failures[0]
	filterReq := httptest.NewRequest(http.MethodGet, "/v1/spotify/webhooks/failures?limit=5&job_id="+url.QueryEscape(target.JobID)+"&event="+url.QueryEscape(target.Event), nil)
	filterRR := httptest.NewRecorder()
	handler.ServeHTTP(filterRR, filterReq)
	if filterRR.Code != http.StatusOK {
		t.Fatalf("expected filtered failures 200, got %d: %s", filterRR.Code, filterRR.Body.String())
	}
	var filteredPayload struct {
		Data struct {
			Count    int                       `json:"count"`
			JobID    string                    `json:"job_id"`
			Event    string                    `json:"event"`
			Failures []pipeline.WebhookFailure `json:"failures"`
		} `json:"data"`
	}
	if err := json.Unmarshal(filterRR.Body.Bytes(), &filteredPayload); err != nil {
		t.Fatalf("unmarshal filtered failures response: %v", err)
	}
	if filteredPayload.Data.JobID != target.JobID || filteredPayload.Data.Event != target.Event {
		t.Fatalf("unexpected filtered query echo: %+v", filteredPayload.Data)
	}
	if filteredPayload.Data.Count < 1 || len(filteredPayload.Data.Failures) < 1 {
		t.Fatalf("expected at least one filtered failure: %+v", filteredPayload.Data)
	}
	for i := range filteredPayload.Data.Failures {
		if filteredPayload.Data.Failures[i].JobID != target.JobID {
			t.Fatalf("unexpected filtered job id: %+v", filteredPayload.Data.Failures[i])
		}
		if filteredPayload.Data.Failures[i].Event != target.Event {
			t.Fatalf("unexpected filtered event: %+v", filteredPayload.Data.Failures[i])
		}
	}

	badBeforeReq := httptest.NewRequest(http.MethodGet, "/v1/spotify/webhooks/failures?before=bad", nil)
	badBeforeRR := httptest.NewRecorder()
	handler.ServeHTTP(badBeforeRR, badBeforeReq)
	if badBeforeRR.Code != http.StatusBadRequest {
		t.Fatalf("expected bad before 400, got %d: %s", badBeforeRR.Code, badBeforeRR.Body.String())
	}

	replayBody := `{"limit":5,"job_id":"` + target.JobID + `","event":"` + target.Event + `"}`
	replayReq := httptest.NewRequest(http.MethodPost, "/v1/spotify/webhooks/replay", strings.NewReader(replayBody))
	replayReq.Header.Set("Content-Type", "application/json")
	replayRR := httptest.NewRecorder()
	handler.ServeHTTP(replayRR, replayReq)
	if replayRR.Code != http.StatusOK {
		t.Fatalf("expected replay 200, got %d: %s", replayRR.Code, replayRR.Body.String())
	}
	var replayPayload struct {
		Data struct {
			Selected  int                           `json:"selected"`
			Attempted int                           `json:"attempted"`
			Succeeded int                           `json:"succeeded"`
			Failed    int                           `json:"failed"`
			Results   []pipeline.WebhookReplayEntry `json:"results"`
		} `json:"data"`
	}
	if err := json.Unmarshal(replayRR.Body.Bytes(), &replayPayload); err != nil {
		t.Fatalf("unmarshal replay response: %v", err)
	}
	if replayPayload.Data.Selected < 1 || replayPayload.Data.Attempted < 1 || replayPayload.Data.Succeeded < 1 || replayPayload.Data.Failed != 0 {
		t.Fatalf("unexpected replay payload: %+v", replayPayload.Data)
	}
	if attempts < 3 {
		t.Fatalf("expected replay to issue another webhook attempt, got %d", attempts)
	}

	replayBadMethodReq := httptest.NewRequest(http.MethodGet, "/v1/spotify/webhooks/replay", nil)
	replayBadMethodRR := httptest.NewRecorder()
	handler.ServeHTTP(replayBadMethodRR, replayBadMethodReq)
	if replayBadMethodRR.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected replay method-not-allowed, got %d", replayBadMethodRR.Code)
	}
}

func waitForTerminalJobStatus(t *testing.T, svc *pipeline.Service, jobID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := svc.GetJob(jobID)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		switch job.Status {
		case "completed", "failed", "partial_failed", "canceled":
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for terminal job status")
}

func waitForStatusViaAPI(t *testing.T, handler *Handler, jobID string, status string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req := httptest.NewRequest(http.MethodGet, "/v1/spotify/jobs/"+jobID, nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("get job failed: code=%d body=%s", rr.Code, rr.Body.String())
		}
		if strings.Contains(rr.Body.String(), `"status":"`+status+`"`) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for status=%s", status)
}
