package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"apiservices/spotify-flac/internal/spotify/pipeline"
)

type Handler struct {
	service *pipeline.Service
}

func NewHandler(service *pipeline.Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/v1/spotify/") {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/spotify/"), "/")
	switch {
	case path == "lookup":
		h.handleLookup(w, r)
	case path == "jobs":
		h.handleJobsCollection(w, r)
	case path == "download-link":
		h.handleGetDownloadLink(w, r)
	case path == "link-cache-stats":
		h.handleLinkCacheStats(w, r)
	case path == "webhooks/metrics":
		h.handleWebhookMetrics(w, r)
	case path == "webhooks/failures":
		h.handleWebhookFailures(w, r)
	case path == "webhooks/replay":
		h.handleWebhookReplay(w, r)
	case strings.HasPrefix(path, "jobs/"):
		h.handleJobResource(w, r, strings.TrimPrefix(path, "jobs/"))
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (h *Handler) handleLookup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req pipeline.LookupInput
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := h.service.Lookup(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": result})
}

func (h *Handler) handleJobsCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req pipeline.CreateJobInput
		if err := decodeJSONBody(w, r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		result, err := h.service.CreateJob(r.Context(), req)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"data": result})
	case http.MethodGet:
		limit := 50
		if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				writeError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = parsed
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": h.service.ListJobs(limit)})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) handleWebhookMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": h.service.GetWebhookMetrics()})
}

func (h *Handler) handleWebhookFailures(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "limit must be an integer")
			return
		}
		limit = parsed
	}
	before := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("before")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "before must be an integer")
			return
		}
		before = parsed
	}
	jobID := strings.TrimSpace(r.URL.Query().Get("job_id"))
	event := strings.TrimSpace(r.URL.Query().Get("event"))
	entries, total, nextBefore, err := h.service.GetWebhookFailuresPage(limit, before, jobID, event)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed reading webhook failures")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"total":       total,
			"before":      before,
			"next_before": nextBefore,
			"job_id":      jobID,
			"event":       event,
			"count":       len(entries),
			"failures":    entries,
		},
	})
}

func (h *Handler) handleWebhookReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req pipeline.ReplayWebhookInput
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := h.service.ReplayWebhookFailures(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed replaying webhook failures")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": result})
}

func (h *Handler) handleJobResource(w http.ResponseWriter, r *http.Request, path string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	jobID := parts[0]

	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		result, err := h.service.GetJob(jobID)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": result})
		return
	}

	switch parts[1] {
	case "events":
		if len(parts) != 2 {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		limit := 100
		if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				writeError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = parsed
		}
		before := 0
		if raw := strings.TrimSpace(r.URL.Query().Get("before")); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				writeError(w, http.StatusBadRequest, "before must be an integer")
				return
			}
			before = parsed
		}
		events, total, nextBefore, err := h.service.GetJobEventsPage(jobID, limit, before)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"data": map[string]any{
				"job_id":      jobID,
				"count":       len(events),
				"total":       total,
				"before":      before,
				"next_before": nextBefore,
				"events":      events,
			},
		})
	case "retry":
		if len(parts) != 2 {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		result, err := h.service.RetryJob(jobID)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": result})
	case "resume":
		if len(parts) != 2 {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		result, err := h.service.ResumeJob(jobID)
		if err != nil {
			msg := err.Error()
			status := http.StatusBadRequest
			if strings.Contains(msg, "job not found") {
				status = http.StatusNotFound
			}
			writeError(w, status, msg)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": result})
	case "cancel":
		if len(parts) != 2 {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		result, err := h.service.CancelJob(jobID)
		if err != nil {
			msg := err.Error()
			status := http.StatusBadRequest
			if strings.Contains(msg, "job not found") {
				status = http.StatusNotFound
			}
			writeError(w, status, msg)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": result})
	case "items":
		h.handleJobItemOutput(w, r, jobID, parts)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (h *Handler) handleJobItemOutput(w http.ResponseWriter, r *http.Request, jobID string, parts []string) {
	if len(parts) != 4 || parts[3] != "output" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	rawIndex := strings.TrimSpace(parts[2])
	index, err := strconv.Atoi(rawIndex)
	if err != nil || index <= 0 {
		writeError(w, http.StatusBadRequest, "item index must be a positive integer")
		return
	}

	output, err := h.service.ReadItemOutput(jobID, index-1)
	if err != nil {
		msg := err.Error()
		status := http.StatusBadRequest
		if strings.Contains(msg, "job not found") || strings.Contains(msg, "item not found") {
			status = http.StatusNotFound
		}
		writeError(w, status, msg)
		return
	}

	w.Header().Set("Content-Type", output.Mime)
	w.Header().Set("Content-Disposition", `attachment; filename="`+output.Filename+`"`)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(output.Data)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"failed to marshal response"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, out any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return errors.New("invalid json body")
	}

	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("json body must contain a single object")
	}
	return nil
}
