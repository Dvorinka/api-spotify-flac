# Testing Download Link Generation

## New API Endpoints

### 1. Get Download Link
**POST** `/v1/spotify/download-link`

```json
{
  "track_id": "4iV5W9uYEdYUVa79Axb7K",
  "quality": "lossless"
}
```

**Response:**
```json
{
  "data": {
    "url": "https://streaming-service.com/track/123.flac",
    "quality": "lossless",
    "source": "tidal",
    "expires_at": "2025-03-15T18:00:00Z",
    "track_id": "4iV5W9uYEdYUVa79Axb7K",
    "source_id": "123456",
    "file_size": 45678901,
    "mime_type": "audio/flac",
    "bit_depth": 16,
    "sample_rate": 44100
  }
}
```

### 2. Link Cache Stats
**GET** `/v1/spotify/link-cache-stats`

**Response:**
```json
{
  "data": {
    "total_cached": 5,
    "generators": 2
  }
}
```

## Modified Job Creation

### Create Job with Link Preference
**POST** `/v1/spotify/jobs`

```json
{
  "items": ["https://open.spotify.com/track/4iV5W9uYEdYUVa79Axb7K"],
  "include_output_base64": true,
  "prefer_links": true
}
```

When `prefer_links: true`, the system will:
1. Try to generate download links first
2. Fall back to traditional download if link generation fails
3. Return link information in job items instead of file data

### Job Item Response with Links
```json
{
  "items": [
    {
      "source_url": "https://open.spotify.com/track/4iV5W9uYEdYUVa79Axb7K",
      "status": "completed",
      "track": {
        "source_url": "https://open.spotify.com/track/4iV5W9uYEdYUVa79Axb7K",
        "platform": "spotify",
        "kind": "track",
        "track_id": "4iV5W9uYEdYUVa79Axb7K",
        "title": "Song Title",
        "artist": "Artist Name",
        "album": "Album Name"
      },
      "download_url": "https://streaming-service.com/track/123.flac",
      "expires_at": "2025-03-15T18:00:00Z",
      "quality": "lossless",
      "source": "tidal",
      "error": "",
      "attempt_count": 1,
      "last_attempted_at": "2025-03-15T14:00:00Z"
    }
  ]
}
```

## Testing Commands

### Start Server
```bash
cd /home/tdvorak/Desktop/PROG_projekty/GOLANG/API's/spotify-flac
go run cmd/spotify/main.go
```

### Test Download Link Generation
```bash
curl -X POST http://localhost:8080/v1/spotify/download-link \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer dev-spotify-key" \
  -d '{"track_id": "4iV5W9uYEdYUVa79Axb7K"}'
```

### Test Job with Link Preference
```bash
curl -X POST http://localhost:8080/v1/spotify/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer dev-spotify-key" \
  -d '{
    "items": ["https://open.spotify.com/track/4iV5W9uYEdYUVa79Axb7K"],
    "include_output_base64": true,
    "prefer_links": true
  }'
```

### Check Cache Stats
```bash
curl -X GET http://localhost:8080/v1/spotify/link-cache-stats \
  -H "Authorization: Bearer dev-spotify-key"
```

## Key Features Implemented

1. **Link Generation**: Tidal and Qobuz integration for direct download URLs
2. **Caching**: Automatic URL caching with 4-hour TTL
3. **Fallback**: Falls back to traditional downloads if links fail
4. **API Extensions**: New endpoints for direct link access
5. **Backward Compatibility**: Existing job system still works
6. **No File Hosting**: Users download directly from streaming services

## Benefits

- **No Storage Costs**: No file hosting on your infrastructure
- **Faster Response**: Link generation is quicker than full downloads
- **Legal Compliance**: No file redistribution
- **Scalable**: Can handle more concurrent requests
- **Flexible**: Supports both link and download modes
