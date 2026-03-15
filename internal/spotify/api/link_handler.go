package api

import (
	"net/http"
)

func (h *Handler) handleGetDownloadLink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		TrackID string `json:"track_id"`
		Quality string `json:"quality,omitempty"`
	}

	if err := decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.TrackID == "" {
		writeError(w, http.StatusBadRequest, "track_id is required")
		return
	}

	link, err := h.service.GetDownloadLink(r.Context(), req.TrackID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if link == nil {
		writeError(w, http.StatusNotFound, "download link not found")
		return
	}

	response := map[string]interface{}{
		"url":         link.URL,
		"quality":     link.Quality,
		"source":      link.Source,
		"expires_at":  link.ExpiresAt,
		"track_id":    link.TrackID,
		"source_id":   link.SourceTrackID,
		"file_size":   link.FileSize,
		"mime_type":   link.MimeType,
		"bit_depth":   link.BitDepth,
		"sample_rate": link.SampleRate,
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"data": response})
}

func (h *Handler) handleLinkCacheStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	stats := h.service.GetLinkCacheStats()
	writeJSON(w, http.StatusOK, map[string]interface{}{"data": stats})
}
