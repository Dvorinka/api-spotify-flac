package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"apiservices/spotify-flac/internal/spotify/api"
	"apiservices/spotify-flac/internal/spotify/auth"
	"apiservices/spotify-flac/internal/spotify/pipeline"
)

func main() {
	logger := log.New(os.Stdout, "[spotify] ", log.LstdFlags)

	port := envString("PORT", "8098")
	apiKey := envString("SPOTIFY_API_KEY", "dev-spotify-key")
	if apiKey == "dev-spotify-key" {
		logger.Println("SPOTIFY_API_KEY not set, using default development key")
	}

	downloaderCmd := envString("SPOTIFY_DOWNLOADER_CMD", "")
	if stringsTrim(downloaderCmd) == "" {
		logger.Println("SPOTIFY_DOWNLOADER_CMD is empty; jobs will fail until downloader command is configured")
	}

	service, err := pipeline.NewService(pipeline.Config{
		DownloaderCmd:  downloaderCmd,
		OutputDir:      envString("SPOTIFY_OUTPUT_DIR", ""),
		WorkerCount:    envInt("SPOTIFY_WORKERS", 1),
		LookupBaseURL:  envString("SPOTIFY_LOOKUP_BASE_URL", "https://open.spotify.com"),
		StateFile:      envString("SPOTIFY_STATE_FILE", ""),
		WebhookURL:     envString("SPOTIFY_WEBHOOK_URL", ""),
		WebhookSecret:  envString("SPOTIFY_WEBHOOK_SECRET", ""),
		WebhookRetries: envInt("SPOTIFY_WEBHOOK_MAX_RETRIES", 3),
		WebhookRetryMS: envInt("SPOTIFY_WEBHOOK_RETRY_MS", 400),
	})
	if err != nil {
		logger.Fatalf("failed to create service: %v", err)
	}
	defer service.Close()

	handler := api.NewHandler(service)

	mux := http.NewServeMux()
	mux.Handle("/v1/spotify/", auth.Middleware(apiKey)(handler))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadTimeout:       12 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Printf("service listening on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("server failed: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("shutdown error: %v", err)
	}
}

func envString(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return parsed
}

func stringsTrim(value string) string {
	for len(value) > 0 && (value[0] == ' ' || value[0] == '\t' || value[0] == '\n' || value[0] == '\r') {
		value = value[1:]
	}
	for len(value) > 0 {
		last := value[len(value)-1]
		if last != ' ' && last != '\t' && last != '\n' && last != '\r' {
			break
		}
		value = value[:len(value)-1]
	}
	return value
}
