package main

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pmessage "csgo-2d-demo-player/pkg/message"
	pparser "csgo-2d-demo-player/pkg/parser"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	replayStateMissing          = "missing"
	replayStateQueued           = "queued"
	replayStateParsing          = "parsing"
	replayStateReady            = "ready"
	replayStateFailed           = "failed"
	defaultWebhookHeader        = "X-Webhook-Secret"
	defaultReplaysDir           = "./parsed"
	defaultStateFileName        = "replay_state.json"
	defaultFaceitDownloadAPIURL = "https://open.faceit.com/download/v2/demos/download"
	maxWebhookBodyBytes         = 1 << 20
	recentProcessingWindowSize  = 10
	minDownloadedDemoBytes      = 4 << 10
)

type replayWork struct {
	EventID         string
	MatchID         string
	MapID           string
	MatchInstanceID string
	DemoURL         string
	Timestamp       time.Time
}

type replayRecord struct {
	MatchID            string    `json:"match_id"`
	MapID              string    `json:"map_id"`
	MatchInstanceID    string    `json:"match_instance_id,omitempty"`
	State              string    `json:"state"`
	ArtifactPath       string    `json:"artifact_path,omitempty"`
	DemoURL            string    `json:"demo_url,omitempty"`
	LastError          string    `json:"last_error,omitempty"`
	LastEventID        string    `json:"last_event_id,omitempty"`
	LastTransactionID  string    `json:"last_transaction_id,omitempty"`
	LastUpdatedAt      time.Time `json:"last_updated_at"`
	CreatedAt          time.Time `json:"created_at"`
	LastWebhookEventAt time.Time `json:"last_webhook_event_at,omitempty"`
}

type replayMetadata struct {
	MatchID           string    `json:"match_id"`
	MapID             string    `json:"map_id"`
	MatchInstanceID   string    `json:"match_instance_id,omitempty"`
	SchemaVersion     int       `json:"schema_version"`
	ParserVersion     string    `json:"parser_version"`
	SourceEventID     string    `json:"source_event_id"`
	SourceReceivedAt  time.Time `json:"source_received_at"`
	CreatedAt         time.Time `json:"created_at"`
	ArtifactPath      string    `json:"artifact_path"`
	ArtifactSizeBytes int64     `json:"artifact_size_bytes"`
	ArtifactKind      string    `json:"artifact_kind"`
	SourceDemoURL     string    `json:"source_demo_url"`
}

type faceitDemoReadyEvent struct {
	TransactionID string `json:"transaction_id"`
	Event         string `json:"event"`
	EventID       string `json:"event_id"`
	Timestamp     string `json:"timestamp"`
	Payload       struct {
		ID              string `json:"id"`
		Game            string `json:"game"`
		Round           int    `json:"round"`
		DemoURL         string `json:"demo_url"`
		MatchInstanceID string `json:"match_instance_id"`
	} `json:"payload"`
}

type replayService struct {
	mu                    sync.RWMutex
	records               map[string]*replayRecord
	latestMapByMatch      map[string]string
	seenEvents            map[string]struct{}
	queueOrder            []string
	activeParses          int
	workerCount           int
	processingDurations   []time.Duration
	queue                 chan replayWork
	replaysDir            string
	stateFile             string
	persistMu             sync.Mutex
	webhookHeaderName     string
	webhookHeaderVal      string
	adminToken            string
	faceitDownloadAPIURL  string
	faceitDownloadAPIKeys []string
	httpClient            *http.Client
}

type faceitDownloadAPIRequest struct {
	ResourceURL string `json:"resource_url"`
}

type faceitDownloadAPIResponse struct {
	Payload struct {
		DownloadURL string `json:"download_url"`
	} `json:"payload"`
}

type replayPersistentState struct {
	Version               int                     `json:"version"`
	Records               map[string]replayRecord `json:"records"`
	LatestMapByMatch      map[string]string       `json:"latest_map_by_match"`
	SeenEvents            []string                `json:"seen_events"`
	QueueOrder            []string                `json:"queue_order"`
	ProcessingDurationsMS []int64                 `json:"processing_durations_ms,omitempty"`
}

func newReplayService() (*replayService, error) {
	replaysDir := strings.TrimSpace(os.Getenv("REPLAYS_DIR"))
	if replaysDir == "" {
		replaysDir = defaultReplaysDir
	}
	if err := os.MkdirAll(replaysDir, 0o755); err != nil {
		return nil, err
	}

	headerName := strings.TrimSpace(os.Getenv("WEBHOOK_HEADER_NAME"))
	if headerName == "" {
		headerName = defaultWebhookHeader
	}

	queueSize := envInt("QUEUE_SIZE", 1024)
	stateFile := strings.TrimSpace(os.Getenv("REPLAY_STATE_FILE"))
	if stateFile == "" {
		stateFile = filepath.Join(replaysDir, defaultStateFileName)
	}
	faceitDownloadAPIURL := strings.TrimSpace(os.Getenv("FACEIT_DOWNLOAD_API_URL"))
	if faceitDownloadAPIURL == "" {
		faceitDownloadAPIURL = defaultFaceitDownloadAPIURL
	}
	faceitDownloadAPIKeys := parseCSVList(os.Getenv("FACEIT_DOWNLOAD_API_KEYS"))

	svc := &replayService{
		records:               map[string]*replayRecord{},
		latestMapByMatch:      map[string]string{},
		seenEvents:            map[string]struct{}{},
		queueOrder:            make([]string, 0),
		workerCount:           1,
		processingDurations:   make([]time.Duration, 0, recentProcessingWindowSize),
		queue:                 make(chan replayWork, queueSize),
		replaysDir:            replaysDir,
		stateFile:             stateFile,
		webhookHeaderName:     headerName,
		webhookHeaderVal:      os.Getenv("WEBHOOK_HEADER_VALUE"),
		adminToken:            os.Getenv("ADMIN_REPROCESS_TOKEN"),
		faceitDownloadAPIURL:  faceitDownloadAPIURL,
		faceitDownloadAPIKeys: faceitDownloadAPIKeys,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return errors.New("redirects not allowed")
			},
		},
	}
	if err := svc.loadState(); err != nil {
		return nil, err
	}
	if len(faceitDownloadAPIKeys) > 0 {
		logger.Info("configured Faceit Downloads API keys", zap.Int("key_count", len(faceitDownloadAPIKeys)))
	}
	return svc, nil
}

func parseCSVList(raw string) []string {
	items := strings.Split(raw, ",")
	out := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func isPappaDemoHost(host string) bool {
	return strings.EqualFold(host, "pappa.aukko.net")
}

func isFaceitDemoHost(host string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" {
		return false
	}
	return strings.Contains(h, "faceit")
}

func (s *replayService) resolveDemoDownloadURL(rawDemoURL string) (string, error) {
	parsed, err := url.Parse(rawDemoURL)
	if err != nil {
		return "", fmt.Errorf("invalid demo url: %w", err)
	}

	host := strings.TrimSpace(parsed.Host)
	if isPappaDemoHost(host) {
		return secureDemoUrl(rawDemoURL, isDev)
	}

	if isFaceitDemoHost(host) {
		if len(s.faceitDownloadAPIKeys) == 0 {
			return "", fmt.Errorf("faceit demo url requires FACEIT_DOWNLOAD_API_KEYS configuration")
		}
		signedURL, err := s.fetchSignedFaceitDemoURL(rawDemoURL)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(signedURL) == "" {
			return "", fmt.Errorf("faceit download api returned an empty signed url")
		}
		return signedURL, nil
	}

	return secureDemoUrl(rawDemoURL, isDev)
}

func (s *replayService) fetchSignedFaceitDemoURL(resourceURL string) (string, error) {
	body := faceitDownloadAPIRequest{ResourceURL: resourceURL}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	var lastErr error
	for _, apiKey := range s.faceitDownloadAPIKeys {
		req, err := http.NewRequest(http.MethodPost, s.faceitDownloadAPIURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("downloads api request failed: %w", err)
			continue
		}

		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("downloads api response read failed: %w", readErr)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("downloads api status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
			continue
		}

		var out faceitDownloadAPIResponse
		if err := json.Unmarshal(respBody, &out); err != nil {
			lastErr = fmt.Errorf("downloads api invalid json response: %w", err)
			continue
		}
		if strings.TrimSpace(out.Payload.DownloadURL) == "" {
			lastErr = fmt.Errorf("downloads api response missing payload.download_url")
			continue
		}

		return out.Payload.DownloadURL, nil
	}

	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("no faceit downloads api keys configured")
}

func eventDedupeKey(eventID string, matchID string, mapID string) string {
	return eventID + "::" + matchID + "::" + mapID
}

func (s *replayService) startWorkers(n int) {
	if n < 1 {
		n = 1
	}
	s.mu.Lock()
	s.workerCount = n
	s.mu.Unlock()
	s.cleanupStaleDownloadTempFiles()
	for i := 0; i < n; i++ {
		go s.workerLoop(i + 1)
	}
	go s.recoverPendingJobs()
}

func (s *replayService) cleanupStaleDownloadTempFiles() {
	entries, err := os.ReadDir(s.replaysDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Error("failed to list replays directory for cleanup", zap.String("replays_dir", s.replaysDir), zap.Error(err))
		}
		return
	}

	removed := 0
	failed := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		matchDir := filepath.Join(s.replaysDir, entry.Name())
		files, err := os.ReadDir(matchDir)
		if err != nil {
			failed++
			logger.Warn("failed to list match directory during stale temp cleanup", zap.String("match_dir", matchDir), zap.Error(err))
			continue
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}
			name := file.Name()
			if !strings.HasPrefix(name, "download-") || !strings.HasSuffix(name, ".dem.zst") {
				continue
			}
			if err := os.Remove(filepath.Join(matchDir, name)); err != nil {
				failed++
				logger.Warn("failed to remove stale download temp file", zap.String("path", filepath.Join(matchDir, name)), zap.Error(err))
				continue
			}
			removed++
		}
	}

	if removed > 0 || failed > 0 {
		logger.Info("completed stale download temp cleanup", zap.Int("removed", removed), zap.Int("failed", failed))
	}
}

func (s *replayService) recoverPendingJobs() {
	now := time.Now().UTC()
	jobsByKey := make(map[string]replayWork)
	jobs := make([]replayWork, 0)

	s.mu.Lock()
	for _, rec := range s.records {
		key := replayRecordKey(rec.MatchID, rec.MapID)
		if rec.State == replayStateParsing {
			rec.State = replayStateQueued
			rec.LastError = ""
			rec.LastUpdatedAt = now
		}
		if rec.State == replayStateQueued && strings.TrimSpace(rec.DemoURL) != "" {
			jobsByKey[key] = replayWork{
				EventID:         rec.LastEventID,
				MatchID:         rec.MatchID,
				MapID:           rec.MapID,
				MatchInstanceID: rec.MatchInstanceID,
				DemoURL:         rec.DemoURL,
				Timestamp:       rec.LastWebhookEventAt,
			}
		}
	}

	recoveredQueueOrder := make([]string, 0, len(jobsByKey))
	seenQueueKeys := make(map[string]struct{})
	for _, key := range s.queueOrder {
		job, ok := jobsByKey[key]
		if !ok {
			continue
		}
		recoveredQueueOrder = append(recoveredQueueOrder, key)
		jobs = append(jobs, job)
		seenQueueKeys[key] = struct{}{}
	}
	for key, job := range jobsByKey {
		if _, exists := seenQueueKeys[key]; exists {
			continue
		}
		recoveredQueueOrder = append(recoveredQueueOrder, key)
		jobs = append(jobs, job)
	}
	s.queueOrder = recoveredQueueOrder
	s.mu.Unlock()

	if len(jobs) == 0 {
		return
	}
	if err := s.persistState(); err != nil {
		logger.Error("failed to persist replay state during startup recovery", zap.Error(err))
	}

	for _, job := range jobs {
		s.queue <- job
	}

	logger.Info("recovered replay jobs from persisted state", zap.Int("jobs", len(jobs)))
}

func (s *replayService) loadState() error {
	data, err := os.ReadFile(s.stateFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	var persisted replayPersistentState
	if err := json.Unmarshal(data, &persisted); err != nil {
		return err
	}

	records := map[string]*replayRecord{}
	normalizedArtifacts := 0
	for key, rec := range persisted.Records {
		recCopy := rec
		if recCopy.State == replayStateReady {
			normalizedPath := s.resolveArtifactPath(recCopy.MatchID, recCopy.MapID, recCopy.ArtifactPath)
			if normalizedPath != recCopy.ArtifactPath {
				recCopy.ArtifactPath = normalizedPath
				normalizedArtifacts++
			}
		}
		records[key] = &recCopy
	}

	latest := persisted.LatestMapByMatch
	if latest == nil {
		latest = map[string]string{}
	}

	seen := map[string]struct{}{}
	for _, key := range persisted.SeenEvents {
		if strings.TrimSpace(key) == "" {
			continue
		}
		seen[key] = struct{}{}
	}
	queueOrder := make([]string, 0, len(persisted.QueueOrder))
	for _, key := range persisted.QueueOrder {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		queueOrder = append(queueOrder, trimmed)
	}

	processingDurations := make([]time.Duration, 0, len(persisted.ProcessingDurationsMS))
	for _, durMS := range persisted.ProcessingDurationsMS {
		if durMS <= 0 {
			continue
		}
		processingDurations = append(processingDurations, time.Duration(durMS)*time.Millisecond)
	}
	if len(processingDurations) > recentProcessingWindowSize {
		processingDurations = processingDurations[len(processingDurations)-recentProcessingWindowSize:]
	}

	s.mu.Lock()
	s.records = records
	s.latestMapByMatch = latest
	s.seenEvents = seen
	s.queueOrder = queueOrder
	s.processingDurations = processingDurations
	s.mu.Unlock()

	logger.Info("loaded replay state", zap.Int("records", len(records)), zap.Int("seen_events", len(seen)))
	if normalizedArtifacts > 0 {
		logger.Info("normalized replay artifact paths from persisted state", zap.Int("normalized_records", normalizedArtifacts))
		if err := s.persistState(); err != nil {
			logger.Error("failed to persist normalized replay state", zap.Error(err))
		}
	}
	return nil
}

func fileExists(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func (s *replayService) resolveArtifactPath(matchID string, mapID string, artifactPath string) string {
	trimmed := strings.TrimSpace(artifactPath)
	if trimmed == "" {
		return ""
	}

	if fileExists(trimmed) {
		return trimmed
	}

	if !filepath.IsAbs(trimmed) {
		joined := filepath.Join(s.replaysDir, trimmed)
		if fileExists(joined) {
			return joined
		}
	}

	if strings.TrimSpace(matchID) != "" && strings.TrimSpace(mapID) != "" {
		expected := filepath.Join(s.replaysDir, matchID, fmt.Sprintf("%s-%s.pbr.gz", matchID, mapID))
		if fileExists(expected) {
			return expected
		}
	}

	return trimmed
}

func (s *replayService) persistState() error {
	s.mu.RLock()
	persisted := replayPersistentState{
		Version:               1,
		Records:               make(map[string]replayRecord, len(s.records)),
		LatestMapByMatch:      make(map[string]string, len(s.latestMapByMatch)),
		SeenEvents:            make([]string, 0, len(s.seenEvents)),
		QueueOrder:            make([]string, 0, len(s.queueOrder)),
		ProcessingDurationsMS: make([]int64, 0, len(s.processingDurations)),
	}
	for key, rec := range s.records {
		persisted.Records[key] = *rec
	}
	for key, value := range s.latestMapByMatch {
		persisted.LatestMapByMatch[key] = value
	}
	for key := range s.seenEvents {
		persisted.SeenEvents = append(persisted.SeenEvents, key)
	}
	persisted.QueueOrder = append(persisted.QueueOrder, s.queueOrder...)
	for _, duration := range s.processingDurations {
		persisted.ProcessingDurationsMS = append(persisted.ProcessingDurationsMS, duration.Milliseconds())
	}
	s.mu.RUnlock()

	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	return writeJSONFileAtomic(s.stateFile, persisted)
}

func (s *replayService) workerLoop(id int) {
	logger.Info("started replay worker", zap.Int("worker_id", id))
	for job := range s.queue {
		s.mu.Lock()
		s.removeFromQueueOrderLocked(replayRecordKey(job.MatchID, job.MapID))
		s.activeParses++
		s.mu.Unlock()
		s.updateState(job.MatchID, job.MapID, replayStateParsing, "", "", "", job.EventID)
		startedAt := time.Now().UTC()

		if err := s.process(job); err != nil {
			s.mu.Lock()
			s.activeParses--
			s.mu.Unlock()
			logger.Error("replay processing failed",
				zap.String("match_id", job.MatchID),
				zap.String("map_id", job.MapID),
				zap.String("event_id", job.EventID),
				zap.Error(err))
			s.updateState(job.MatchID, job.MapID, replayStateFailed, "", err.Error(), "", job.EventID)
			continue
		}

		s.mu.Lock()
		s.activeParses--
		s.mu.Unlock()
		s.recordProcessingDuration(time.Since(startedAt))
	}
}

func (s *replayService) queuePositionLocked(matchID string, mapID string, state string) (int, int) {
	key := replayRecordKey(matchID, mapID)
	total := len(s.queueOrder)
	if state == replayStateParsing {
		return 0, total
	}
	for i, queuedKey := range s.queueOrder {
		if queuedKey == key {
			return i + 1, total
		}
	}
	return -1, total
}

func (s *replayService) recordProcessingDuration(duration time.Duration) {
	if duration <= 0 {
		return
	}

	s.mu.Lock()
	s.processingDurations = append(s.processingDurations, duration)
	if len(s.processingDurations) > recentProcessingWindowSize {
		s.processingDurations = s.processingDurations[len(s.processingDurations)-recentProcessingWindowSize:]
	}
	s.mu.Unlock()

	if err := s.persistState(); err != nil {
		logger.Error("failed to persist replay state", zap.Error(err))
	}
}

func (s *replayService) averageProcessingDurationLocked() (time.Duration, bool) {
	if len(s.processingDurations) == 0 {
		return 0, false
	}

	var total time.Duration
	for _, duration := range s.processingDurations {
		total += duration
	}

	return total / time.Duration(len(s.processingDurations)), true
}

func (s *replayService) etaMinutesLocked(state string, queuePosition int) (int, bool) {
	avgDuration, ok := s.averageProcessingDurationLocked()
	if !ok || avgDuration <= 0 {
		return 0, false
	}

	workers := s.workerCount
	if workers < 1 {
		workers = 1
	}

	jobsUntilReady := 0
	switch state {
	case replayStateParsing:
		jobsUntilReady = 1
	case replayStateQueued:
		if queuePosition < 1 {
			return 0, false
		}
		jobsUntilReady = s.activeParses + queuePosition
	default:
		return 0, false
	}

	estimatedDuration := time.Duration(ceilDiv(jobsUntilReady, workers)) * avgDuration
	estimatedMinutes := int((estimatedDuration + time.Minute - 1) / time.Minute)
	if estimatedMinutes < 1 {
		estimatedMinutes = 1
	}

	return estimatedMinutes, true
}

func ceilDiv(n int, d int) int {
	if d <= 0 {
		return 0
	}
	return (n + d - 1) / d
}

func (s *replayService) hasQueueKeyLocked(key string) bool {
	for _, queuedKey := range s.queueOrder {
		if queuedKey == key {
			return true
		}
	}
	return false
}

func (s *replayService) appendQueueOrderLocked(key string) {
	if s.hasQueueKeyLocked(key) {
		return
	}
	s.queueOrder = append(s.queueOrder, key)
}

func (s *replayService) removeFromQueueOrderLocked(key string) {
	for i, queuedKey := range s.queueOrder {
		if queuedKey != key {
			continue
		}
		s.queueOrder = append(s.queueOrder[:i], s.queueOrder[i+1:]...)
		return
	}
}

func (s *replayService) removeSeenEventsLocked(matchID string, mapID string) int {
	suffix := "::" + matchID + "::" + mapID
	removed := 0
	for key := range s.seenEvents {
		if !strings.HasSuffix(key, suffix) {
			continue
		}
		delete(s.seenEvents, key)
		removed++
	}
	return removed
}

func (s *replayService) process(job replayWork) error {
	secureURL, err := s.resolveDemoDownloadURL(job.DemoURL)
	if err != nil {
		return fmt.Errorf("invalid demo url: %w", err)
	}
	parseFilename := filepath.Base(secureURL)
	if parsedURL, parseErr := url.Parse(secureURL); parseErr == nil {
		if base := filepath.Base(parsedURL.Path); strings.TrimSpace(base) != "" && base != "." && base != "/" {
			parseFilename = base
		}
	}

	matchDir := filepath.Join(s.replaysDir, job.MatchID)
	if err := os.MkdirAll(matchDir, 0o755); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(matchDir, "download-*.dem.zst")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}()

	resp, err := s.httpClient.Get(secureURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("demo download status: %d", resp.StatusCode)
	}

	size, err := io.Copy(tmpFile, resp.Body)
	if err != nil {
		return err
	}
	if size < minDownloadedDemoBytes {
		return fmt.Errorf("downloaded demo too small: %d bytes", size)
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := validateDemoFilestamp(tmpFile, parseFilename); err != nil {
		return err
	}

	// Parse demo into a compact replay artifact containing only emitted playback messages.
	replayTmp, err := os.CreateTemp(matchDir, "replay-*.pbr.gz")
	if err != nil {
		return err
	}
	replayTmpPath := replayTmp.Name()
	defer func() {
		_ = replayTmp.Close()
		_ = os.Remove(replayTmpPath)
	}()

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	gzWriter := gzip.NewWriter(replayTmp)
	var frameLen [4]byte
	messageCount := 0
	reducedSizeBytes := int64(0)
	var callbackErr error

	parseErr := pparser.WasmParseDemo(parseFilename, tmpFile, func(payload []byte) {
		if callbackErr != nil {
			return
		}

		msg := &pmessage.Message{}
		if err := proto.Unmarshal(payload, msg); err != nil {
			callbackErr = err
			return
		}
		// EmptyType messages only keep frame progression and are not needed in persisted artifacts.
		if msg.MsgType == pmessage.Message_EmptyType {
			return
		}

		binary.LittleEndian.PutUint32(frameLen[:], uint32(len(payload)))
		if _, err := gzWriter.Write(frameLen[:]); err != nil {
			callbackErr = err
			return
		}
		if _, err := gzWriter.Write(payload); err != nil {
			callbackErr = err
			return
		}
		messageCount++
		reducedSizeBytes += int64(4 + len(payload))
	})
	if callbackErr != nil {
		return callbackErr
	}
	if parseErr != nil {
		return fmt.Errorf("parse failed: %w", parseErr)
	}

	if err := gzWriter.Close(); err != nil {
		return err
	}
	if err := replayTmp.Sync(); err != nil {
		return err
	}
	if err := replayTmp.Close(); err != nil {
		return err
	}

	artifactName := fmt.Sprintf("%s-%s.pbr.gz", job.MatchID, job.MapID)
	artifactPath := filepath.Join(matchDir, artifactName)
	if err := os.Rename(replayTmpPath, artifactPath); err != nil {
		return err
	}

	artifactInfo, err := os.Stat(artifactPath)
	if err != nil {
		return err
	}

	meta := replayMetadata{
		MatchID:           job.MatchID,
		MapID:             job.MapID,
		MatchInstanceID:   job.MatchInstanceID,
		SchemaVersion:     1,
		ParserVersion:     "ingest-v2",
		SourceEventID:     job.EventID,
		SourceReceivedAt:  job.Timestamp,
		CreatedAt:         time.Now().UTC(),
		ArtifactPath:      artifactPath,
		ArtifactSizeBytes: artifactInfo.Size(),
		ArtifactKind:      "proto_replay_stream_gzip_v1",
		SourceDemoURL:     secureURL,
	}
	metaPath := filepath.Join(matchDir, "metadata.json")
	if err := writeJSONFileAtomic(metaPath, meta); err != nil {
		return err
	}

	logger.Info("replay parsed and stored",
		zap.String("match_id", job.MatchID),
		zap.String("map_id", job.MapID),
		zap.Int("messages", messageCount),
		zap.Int64("source_size_bytes", size),
		zap.Int64("reduced_payload_bytes", reducedSizeBytes),
		zap.Int64("artifact_size_bytes", artifactInfo.Size()))

	s.updateState(job.MatchID, job.MapID, replayStateReady, artifactPath, "", secureURL, job.EventID)
	return nil
}

type demoReadCloser struct {
	reader  io.Reader
	closeFn func() error
}

func (d *demoReadCloser) Read(p []byte) (int, error) {
	return d.reader.Read(p)
}

func (d *demoReadCloser) Close() error {
	if d.closeFn == nil {
		return nil
	}
	return d.closeFn()
}

func openDemoStream(filename string, src *os.File) (io.ReadCloser, error) {
	if strings.HasSuffix(filename, ".gz") {
		r, err := gzip.NewReader(src)
		if err != nil {
			return nil, err
		}
		return r, nil
	}

	if strings.HasSuffix(filename, ".zst") {
		r, err := zstd.NewReader(src)
		if err != nil {
			return nil, err
		}
		return r.IOReadCloser(), nil
	}

	if strings.HasSuffix(filename, ".bz2") {
		return &demoReadCloser{reader: bzip2.NewReader(src)}, nil
	}

	if strings.HasSuffix(filename, ".dem") {
		return &demoReadCloser{reader: src}, nil
	}

	return nil, fmt.Errorf("unsupported file format %s", filename)
}

func validateDemoFilestamp(file *os.File, filename string) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	r, err := openDemoStream(filename, file)
	if err != nil {
		return err
	}
	defer r.Close()

	stamp := make([]byte, 8)
	if _, err := io.ReadFull(r, stamp); err != nil {
		return fmt.Errorf("failed to read demo filestamp: %w", err)
	}

	if !bytes.HasPrefix(stamp, []byte("PBDEMS2")) {
		return fmt.Errorf("invalid demo filestamp %q", string(stamp))
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return nil
}

func (s *replayService) updateState(matchID string, mapID string, state string, artifactPath string, lastError string, demoURL string, eventID string) {
	now := time.Now().UTC()
	s.mu.Lock()

	rec := s.getOrCreateLocked(matchID, mapID)
	rec.MapID = mapID
	rec.State = state
	rec.LastUpdatedAt = now
	if artifactPath != "" {
		rec.ArtifactPath = artifactPath
	}
	if lastError != "" {
		rec.LastError = lastError
	} else {
		rec.LastError = ""
	}
	if demoURL != "" {
		rec.DemoURL = demoURL
	}
	if eventID != "" {
		rec.LastEventID = eventID
	}
	s.latestMapByMatch[matchID] = mapID
	s.mu.Unlock()

	if err := s.persistState(); err != nil {
		logger.Error("failed to persist replay state", zap.Error(err))
	}
}

func replayRecordKey(matchID string, mapID string) string {
	return matchID + "::" + mapID
}

func (s *replayService) getOrCreateLocked(matchID string, mapID string) *replayRecord {
	key := replayRecordKey(matchID, mapID)
	rec, ok := s.records[key]
	if ok {
		return rec
	}
	now := time.Now().UTC()
	rec = &replayRecord{
		MatchID:       matchID,
		MapID:         mapID,
		State:         replayStateMissing,
		CreatedAt:     now,
		LastUpdatedAt: now,
	}
	s.records[key] = rec
	s.latestMapByMatch[matchID] = mapID
	return rec
}

func (s *replayService) getLocked(matchID string, mapID string) (*replayRecord, bool) {
	if mapID != "" {
		rec, ok := s.records[replayRecordKey(matchID, mapID)]
		return rec, ok
	}

	if latestMapID, ok := s.latestMapByMatch[matchID]; ok {
		rec, found := s.records[replayRecordKey(matchID, latestMapID)]
		if found {
			return rec, true
		}
	}

	for _, rec := range s.records {
		if rec.MatchID == matchID {
			return rec, true
		}
	}

	return nil, false
}

func (s *replayService) mapIDsLocked(matchID string) []string {
	mapIDs := make([]string, 0)
	for _, rec := range s.records {
		if rec.MatchID == matchID {
			mapIDs = append(mapIDs, rec.MapID)
		}
	}
	return mapIDs
}

func (s *replayService) webhookDemoReadyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.webhookAuthorized(r) {
		http.Error(w, "unauthorized webhook", http.StatusUnauthorized)
		return
	}

	body := http.MaxBytesReader(w, r.Body, maxWebhookBodyBytes)
	defer body.Close()

	var event faceitDemoReadyEvent
	if err := json.NewDecoder(body).Decode(&event); err != nil {
		http.Error(w, "invalid json payload", http.StatusBadRequest)
		return
	}

	if err := validateFaceitDemoReady(event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	timestamp, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		timestamp = time.Now().UTC()
	}
	mapID := mapIDFromEvent(event)
	if mapID == "" {
		mapID = "unknown"
	}
	dedupeKey := eventDedupeKey(event.EventID, event.Payload.ID, mapID)
	now := time.Now().UTC()

	s.mu.Lock()
	if _, exists := s.seenEvents[dedupeKey]; exists {
		rec, ok := s.getLocked(event.Payload.ID, mapID)
		state := replayStateMissing
		if ok {
			state = rec.State
		}
		resp := map[string]string{
			"match_id": event.Payload.ID,
			"map_id":   mapID,
			"state":    state,
			"result":   "duplicate",
		}
		s.mu.Unlock()
		writeJSON(w, http.StatusAccepted, resp)
		return
	}
	rec := s.getOrCreateLocked(event.Payload.ID, mapID)
	rec.MatchID = event.Payload.ID
	rec.MapID = mapID
	rec.MatchInstanceID = event.Payload.MatchInstanceID
	rec.LastTransactionID = event.TransactionID
	rec.LastWebhookEventAt = timestamp
	rec.DemoURL = event.Payload.DemoURL
	rec.LastEventID = event.EventID
	rec.LastUpdatedAt = now
	if rec.State == replayStateReady && strings.TrimSpace(rec.ArtifactPath) != "" {
		rec.LastError = ""
		s.latestMapByMatch[event.Payload.ID] = mapID
		s.mu.Unlock()
		if err := s.persistState(); err != nil {
			logger.Error("failed to persist replay state", zap.Error(err))
		}
		writeJSON(w, http.StatusAccepted, map[string]string{
			"match_id": event.Payload.ID,
			"map_id":   mapID,
			"state":    replayStateReady,
			"result":   "already_ready",
		})
		return
	}
	if rec.State == replayStateQueued || rec.State == replayStateParsing {
		rec.LastError = ""
		if rec.State == replayStateQueued {
			s.appendQueueOrderLocked(replayRecordKey(event.Payload.ID, mapID))
		}
		s.latestMapByMatch[event.Payload.ID] = mapID
		s.seenEvents[dedupeKey] = struct{}{}
		state := rec.State
		s.mu.Unlock()
		if err := s.persistState(); err != nil {
			logger.Error("failed to persist replay state", zap.Error(err))
		}
		writeJSON(w, http.StatusAccepted, map[string]string{
			"match_id": event.Payload.ID,
			"map_id":   mapID,
			"state":    state,
		})
		return
	}
	s.mu.Unlock()

	job := replayWork{
		EventID:         event.EventID,
		MatchID:         event.Payload.ID,
		MapID:           mapID,
		MatchInstanceID: event.Payload.MatchInstanceID,
		DemoURL:         event.Payload.DemoURL,
		Timestamp:       timestamp,
	}

	select {
	case s.queue <- job:
		s.mu.Lock()
		rec := s.getOrCreateLocked(event.Payload.ID, mapID)
		rec.MatchID = event.Payload.ID
		rec.MapID = mapID
		rec.MatchInstanceID = event.Payload.MatchInstanceID
		rec.LastTransactionID = event.TransactionID
		rec.LastWebhookEventAt = timestamp
		rec.DemoURL = event.Payload.DemoURL
		rec.LastEventID = event.EventID
		rec.LastError = ""
		rec.State = replayStateQueued
		rec.LastUpdatedAt = now
		s.appendQueueOrderLocked(replayRecordKey(event.Payload.ID, mapID))
		s.latestMapByMatch[event.Payload.ID] = mapID
		s.seenEvents[dedupeKey] = struct{}{}
		s.mu.Unlock()
		if err := s.persistState(); err != nil {
			logger.Error("failed to persist replay state", zap.Error(err))
		}
		writeJSON(w, http.StatusAccepted, map[string]string{
			"match_id": event.Payload.ID,
			"map_id":   mapID,
			"state":    replayStateQueued,
		})
	default:
		s.mu.RLock()
		rec, ok := s.getLocked(event.Payload.ID, mapID)
		state := replayStateMissing
		if ok {
			state = rec.State
		}
		s.mu.RUnlock()
		if state == replayStateQueued || state == replayStateParsing {
			writeJSON(w, http.StatusAccepted, map[string]string{
				"match_id": event.Payload.ID,
				"map_id":   mapID,
				"state":    state,
			})
			return
		}
		s.updateState(event.Payload.ID, mapID, replayStateFailed, "", "queue full", event.Payload.DemoURL, event.EventID)
		http.Error(w, "ingest queue is full", http.StatusServiceUnavailable)
	}
}

func (s *replayService) replaysHandler(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/replays/") {
		http.NotFound(w, r)
		return
	}

	trimmed := strings.TrimPrefix(r.URL.Path, "/replays/")
	trimmed = strings.Trim(trimmed, "/")
	if trimmed == "" {
		http.NotFound(w, r)
		return
	}

	if strings.HasSuffix(trimmed, "/status") {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		matchID := strings.TrimSuffix(trimmed, "/status")
		matchID = strings.Trim(matchID, "/")
		mapID := strings.TrimSpace(r.URL.Query().Get("map_id"))
		s.handleReplayStatus(w, r, matchID, mapID)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	mapID := strings.TrimSpace(r.URL.Query().Get("map_id"))
	s.handleReplayFetch(w, r, trimmed, mapID)
}

func (s *replayService) handleReplayStatus(w http.ResponseWriter, _ *http.Request, matchID string, mapID string) {
	s.mu.RLock()
	rec, ok := s.getLocked(matchID, mapID)
	mapIDs := s.mapIDsLocked(matchID)
	queuePosition, queueTotal := -1, 0
	etaMinutes := -1
	if ok {
		queuePosition, queueTotal = s.queuePositionLocked(rec.MatchID, rec.MapID, rec.State)
		if estimatedMinutes, hasETA := s.etaMinutesLocked(rec.State, queuePosition); hasETA {
			etaMinutes = estimatedMinutes
		}
	}
	s.mu.RUnlock()
	if !ok {
		if recovered, err := s.recoverMatchRecordsFromDisk(matchID); err != nil {
			logger.Error("failed to recover replay records from disk", zap.String("match_id", matchID), zap.Error(err))
		} else if recovered {
			s.mu.RLock()
			rec, ok = s.getLocked(matchID, mapID)
			mapIDs = s.mapIDsLocked(matchID)
			queuePosition, queueTotal = -1, 0
			etaMinutes = -1
			if ok {
				queuePosition, queueTotal = s.queuePositionLocked(rec.MatchID, rec.MapID, rec.State)
				if estimatedMinutes, hasETA := s.etaMinutesLocked(rec.State, queuePosition); hasETA {
					etaMinutes = estimatedMinutes
				}
			}
			s.mu.RUnlock()
		}
	}
	if !ok {
		resp := map[string]any{
			"match_id": matchID,
			"map_id":   mapID,
			"state":    replayStateMissing,
			"map_ids":  mapIDs,
		}
		writeJSON(w, http.StatusNotFound, resp)
		return
	}

	resp := map[string]any{
		"match_id":              rec.MatchID,
		"map_id":                rec.MapID,
		"match_instance_id":     rec.MatchInstanceID,
		"state":                 rec.State,
		"artifact_path":         rec.ArtifactPath,
		"demo_url":              rec.DemoURL,
		"last_error":            rec.LastError,
		"last_event_id":         rec.LastEventID,
		"last_transaction_id":   rec.LastTransactionID,
		"last_updated_at":       rec.LastUpdatedAt,
		"created_at":            rec.CreatedAt,
		"last_webhook_event_at": rec.LastWebhookEventAt,
		"map_ids":               mapIDs,
	}
	if rec.State == replayStateQueued || rec.State == replayStateParsing {
		if queuePosition >= 0 {
			resp["queue_position"] = queuePosition
		}
		resp["queue_total"] = queueTotal
		if etaMinutes >= 0 {
			resp["eta_minutes"] = etaMinutes
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *replayService) handleReplayFetch(w http.ResponseWriter, r *http.Request, matchID string, mapID string) {
	s.mu.RLock()
	rec, ok := s.getLocked(matchID, mapID)
	s.mu.RUnlock()
	if !ok {
		if recovered, err := s.recoverMatchRecordsFromDisk(matchID); err != nil {
			logger.Error("failed to recover replay records from disk", zap.String("match_id", matchID), zap.Error(err))
		} else if recovered {
			s.mu.RLock()
			rec, ok = s.getLocked(matchID, mapID)
			s.mu.RUnlock()
		}
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"match_id": matchID,
			"map_id":   mapID,
			"state":    replayStateMissing,
		})
		return
	}
	if rec.State != replayStateReady || rec.ArtifactPath == "" {
		s.mu.RLock()
		queuePosition, queueTotal := s.queuePositionLocked(rec.MatchID, rec.MapID, rec.State)
		etaMinutes, hasETA := s.etaMinutesLocked(rec.State, queuePosition)
		s.mu.RUnlock()
		resp := map[string]any{
			"match_id": rec.MatchID,
			"map_id":   rec.MapID,
			"state":    rec.State,
			"error":    rec.LastError,
		}
		if rec.State == replayStateQueued || rec.State == replayStateParsing {
			if queuePosition >= 0 {
				resp["queue_position"] = queuePosition
			}
			resp["queue_total"] = queueTotal
			if hasETA {
				resp["eta_minutes"] = etaMinutes
			}
		}
		writeJSON(w, http.StatusAccepted, resp)
		return
	}

	resolvedArtifactPath := s.resolveArtifactPath(rec.MatchID, rec.MapID, rec.ArtifactPath)
	if resolvedArtifactPath != rec.ArtifactPath {
		s.mu.Lock()
		if current, found := s.records[replayRecordKey(rec.MatchID, rec.MapID)]; found {
			current.ArtifactPath = resolvedArtifactPath
			current.LastUpdatedAt = time.Now().UTC()
			rec = current
		}
		s.mu.Unlock()
		if err := s.persistState(); err != nil {
			logger.Error("failed to persist replay state", zap.Error(err))
		}
	}

	if !fileExists(rec.ArtifactPath) {
		if recovered, err := s.recoverMatchRecordsFromDisk(rec.MatchID); err != nil {
			logger.Error("failed to recover replay records from disk", zap.String("match_id", rec.MatchID), zap.Error(err))
		} else if recovered {
			s.mu.RLock()
			rec, ok = s.getLocked(matchID, mapID)
			s.mu.RUnlock()
		}
	}

	if !ok || rec.State != replayStateReady || rec.ArtifactPath == "" || !fileExists(rec.ArtifactPath) {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"match_id": matchID,
			"map_id":   mapID,
			"state":    replayStateMissing,
		})
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Replay-State", rec.State)
	w.Header().Set("X-Replay-Map-Id", rec.MapID)
	w.Header().Set("Cache-Control", "public, max-age=300")
	http.ServeFile(w, r, rec.ArtifactPath)
}

func chooseLatestMapID(mapIDs []string) string {
	if len(mapIDs) == 0 {
		return ""
	}
	copied := append([]string(nil), mapIDs...)
	sort.Slice(copied, func(i, j int) bool {
		leftNum, leftErr := strconv.Atoi(copied[i])
		rightNum, rightErr := strconv.Atoi(copied[j])
		if leftErr == nil && rightErr == nil {
			return leftNum < rightNum
		}
		if leftErr == nil {
			return false
		}
		if rightErr == nil {
			return true
		}
		return copied[i] < copied[j]
	})
	return copied[len(copied)-1]
}

func (s *replayService) discoverArtifacts(matchID string) (map[string]string, error) {
	matchDir := filepath.Join(s.replaysDir, matchID)
	entries, err := os.ReadDir(matchDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	artifacts := make(map[string]string)
	prefix := matchID + "-"
	suffix := ".pbr.gz"
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		mapID := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		if strings.TrimSpace(mapID) == "" {
			continue
		}
		artifacts[mapID] = filepath.Join(matchDir, name)
	}

	return artifacts, nil
}

func (s *replayService) recoverMatchRecordsFromDisk(matchID string) (bool, error) {
	artifacts, err := s.discoverArtifacts(matchID)
	if err != nil {
		return false, err
	}
	if len(artifacts) == 0 {
		return false, nil
	}

	now := time.Now().UTC()
	mapIDs := make([]string, 0, len(artifacts))

	s.mu.Lock()
	changed := false
	for mapID, artifactPath := range artifacts {
		mapIDs = append(mapIDs, mapID)
		key := replayRecordKey(matchID, mapID)
		rec, exists := s.records[key]
		if !exists {
			rec = &replayRecord{
				MatchID:       matchID,
				MapID:         mapID,
				CreatedAt:     now,
				LastUpdatedAt: now,
			}
			s.records[key] = rec
			changed = true
		}
		if rec.State != replayStateReady {
			rec.State = replayStateReady
			changed = true
		}
		if rec.ArtifactPath != artifactPath {
			rec.ArtifactPath = artifactPath
			changed = true
		}
		if rec.LastError != "" {
			rec.LastError = ""
			changed = true
		}
		rec.LastUpdatedAt = now
	}
	if latest := chooseLatestMapID(mapIDs); latest != "" {
		if s.latestMapByMatch[matchID] != latest {
			s.latestMapByMatch[matchID] = latest
			changed = true
		}
	}
	s.mu.Unlock()

	if changed {
		if err := s.persistState(); err != nil {
			return true, err
		}
	}

	return true, nil
}

func (s *replayService) adminReprocessHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.adminAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		MatchID string `json:"match_id"`
		MapID   string `json:"map_id"`
		DemoURL string `json:"demo_url"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&req); err != nil {
		http.Error(w, "invalid json payload", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.MatchID) == "" || strings.TrimSpace(req.DemoURL) == "" {
		http.Error(w, "match_id and demo_url are required", http.StatusBadRequest)
		return
	}
	mapID := strings.TrimSpace(req.MapID)
	if mapID == "" {
		mapID = "manual"
	}
	job := replayWork{
		EventID:   "manual-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		MatchID:   req.MatchID,
		MapID:     mapID,
		DemoURL:   req.DemoURL,
		Timestamp: time.Now().UTC(),
	}

	if !s.enqueueAdminReplayJob(job, "manual") {
		http.Error(w, "ingest queue is full", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{
		"match_id": req.MatchID,
		"map_id":   mapID,
		"state":    replayStateQueued,
	})
}

func (s *replayService) adminReprocessFailedHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.adminAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		MatchID string `json:"match_id"`
		MapID   string `json:"map_id"`
		Limit   int    `json:"limit"`
		DryRun  bool   `json:"dry_run"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "invalid json payload", http.StatusBadRequest)
		return
	}
	if req.Limit < 0 {
		http.Error(w, "limit must be >= 0", http.StatusBadRequest)
		return
	}
	matchID := strings.TrimSpace(req.MatchID)
	mapID := strings.TrimSpace(req.MapID)

	type candidate struct {
		job           replayWork
		transactionID string
	}
	candidates := make([]candidate, 0)
	scannedFailed := 0
	skippedNoDemoURL := 0

	s.mu.RLock()
	for _, rec := range s.records {
		if rec.State != replayStateFailed {
			continue
		}
		scannedFailed++
		if matchID != "" && rec.MatchID != matchID {
			continue
		}
		if mapID != "" && rec.MapID != mapID {
			continue
		}
		if strings.TrimSpace(rec.DemoURL) == "" {
			skippedNoDemoURL++
			continue
		}
		candidates = append(candidates, candidate{
			job: replayWork{
				EventID:         "admin-reprocess-failed-" + strconv.FormatInt(time.Now().UnixNano(), 10),
				MatchID:         rec.MatchID,
				MapID:           rec.MapID,
				MatchInstanceID: rec.MatchInstanceID,
				DemoURL:         rec.DemoURL,
				Timestamp:       time.Now().UTC(),
			},
			transactionID: rec.LastTransactionID,
		})
	}
	s.mu.RUnlock()

	matched := len(candidates)
	if req.Limit > 0 && len(candidates) > req.Limit {
		candidates = candidates[:req.Limit]
	}
	selected := len(candidates)

	if req.DryRun {
		writeJSON(w, http.StatusOK, map[string]any{
			"dry_run":             true,
			"failed_scanned":      scannedFailed,
			"failed_matched":      matched,
			"failed_selected":     selected,
			"queued":              0,
			"queue_full":          0,
			"skipped_no_demo_url": skippedNoDemoURL,
		})
		return
	}

	queued := 0
	queueFull := 0
	for _, c := range candidates {
		if s.enqueueAdminReplayJob(c.job, c.transactionID) {
			queued++
			continue
		}
		queueFull++
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"dry_run":             false,
		"failed_scanned":      scannedFailed,
		"failed_matched":      matched,
		"failed_selected":     selected,
		"queued":              queued,
		"queue_full":          queueFull,
		"skipped_no_demo_url": skippedNoDemoURL,
	})
}

func (s *replayService) adminDeleteReplayHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.adminAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		MatchID string `json:"match_id"`
		MapID   string `json:"map_id"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&req); err != nil {
		http.Error(w, "invalid json payload", http.StatusBadRequest)
		return
	}
	matchID := strings.TrimSpace(req.MatchID)
	mapID := strings.TrimSpace(req.MapID)
	if matchID == "" || mapID == "" {
		http.Error(w, "match_id and map_id are required", http.StatusBadRequest)
		return
	}

	key := replayRecordKey(matchID, mapID)

	s.mu.Lock()
	rec, exists := s.records[key]
	if !exists {
		s.mu.Unlock()
		writeJSON(w, http.StatusNotFound, map[string]any{
			"match_id": matchID,
			"map_id":   mapID,
			"deleted":  false,
			"error":    "replay record not found",
		})
		return
	}
	delete(s.records, key)
	s.removeFromQueueOrderLocked(key)
	removedSeenEvents := s.removeSeenEventsLocked(matchID, mapID)
	if latest := chooseLatestMapID(s.mapIDsLocked(matchID)); latest != "" {
		s.latestMapByMatch[matchID] = latest
	} else {
		delete(s.latestMapByMatch, matchID)
	}
	deletedState := rec.State
	s.mu.Unlock()

	if err := s.persistState(); err != nil {
		logger.Error("failed to persist replay state", zap.Error(err))
		http.Error(w, "failed to persist state", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"match_id":            matchID,
		"map_id":              mapID,
		"deleted":             true,
		"previous_state":      deletedState,
		"removed_seen_events": removedSeenEvents,
	})
}

func (s *replayService) enqueueAdminReplayJob(job replayWork, transactionID string) bool {
	select {
	case s.queue <- job:
		now := time.Now().UTC()
		s.mu.Lock()
		rec := s.getOrCreateLocked(job.MatchID, job.MapID)
		rec.MatchID = job.MatchID
		rec.MapID = job.MapID
		rec.MatchInstanceID = job.MatchInstanceID
		rec.DemoURL = job.DemoURL
		rec.LastEventID = job.EventID
		rec.LastTransactionID = transactionID
		rec.LastWebhookEventAt = job.Timestamp
		rec.LastError = ""
		rec.State = replayStateQueued
		rec.LastUpdatedAt = now
		s.appendQueueOrderLocked(replayRecordKey(job.MatchID, job.MapID))
		s.latestMapByMatch[job.MatchID] = job.MapID
		s.mu.Unlock()
		if err := s.persistState(); err != nil {
			logger.Error("failed to persist replay state", zap.Error(err))
		}
		return true
	default:
		return false
	}
}

func (s *replayService) webhookAuthorized(r *http.Request) bool {
	if s.webhookHeaderVal == "" {
		logger.Warn("webhook secret missing, rejecting all webhook requests")
		return false
	}
	got := r.Header.Get(s.webhookHeaderName)
	if got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.webhookHeaderVal)) == 1
}

func (s *replayService) adminAuthorized(r *http.Request) bool {
	if s.adminToken == "" {
		return false
	}
	got := r.Header.Get("X-Admin-Token")
	if got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.adminToken)) == 1
}

func validateFaceitDemoReady(event faceitDemoReadyEvent) error {
	if event.Event != "match_demo_ready" {
		return fmt.Errorf("unsupported event %q", event.Event)
	}
	if strings.TrimSpace(event.EventID) == "" {
		return fmt.Errorf("event_id is required")
	}
	if strings.TrimSpace(event.Payload.ID) == "" {
		return fmt.Errorf("payload.id is required")
	}
	if strings.ToLower(strings.TrimSpace(event.Payload.Game)) != "cs2" {
		return fmt.Errorf("payload.game must be cs2")
	}
	if strings.TrimSpace(event.Payload.DemoURL) == "" {
		return fmt.Errorf("payload.demo_url is required")
	}
	if strings.TrimSpace(event.Payload.MatchInstanceID) == "" && event.Payload.Round <= 0 && mapIDFromDemoURL(event.Payload.DemoURL) == "" {
		return fmt.Errorf("either payload.match_instance_id, payload.round, or map id in payload.demo_url is required")
	}
	return nil
}

func mapIDFromEvent(event faceitDemoReadyEvent) string {
	if event.Payload.Round > 0 {
		return strconv.Itoa(event.Payload.Round)
	}
	if mapID := mapIDFromMatchInstance(event.Payload.ID, event.Payload.MatchInstanceID, 0); mapID != "" {
		return mapID
	}
	return mapIDFromDemoURL(event.Payload.DemoURL)
}

func mapIDFromDemoURL(demoURL string) string {
	re := regexp.MustCompile(`-(\d+)(?:-\d+)?\.dem\.`)
	matches := re.FindStringSubmatch(demoURL)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

func mapIDFromMatchInstance(matchID string, matchInstanceID string, fallbackRound int) string {
	if matchID != "" {
		prefix := matchID + "-"
		if strings.HasPrefix(matchInstanceID, prefix) {
			rest := strings.TrimPrefix(matchInstanceID, prefix)
			parts := strings.Split(rest, "-")
			if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
				return parts[0]
			}
		}
	}
	if fallbackRound > 0 {
		return strconv.Itoa(fallbackRound)
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logger.Error("failed to write json response", zap.Error(err))
	}
}

func writeJSONFileAtomic(path string, v any) error {
	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, "meta-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}()

	enc := json.NewEncoder(tmpFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}
