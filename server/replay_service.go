package main

import (
	"compress/gzip"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	pmessage "csgo-2d-demo-player/pkg/message"
	pparser "csgo-2d-demo-player/pkg/parser"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	replayStateMissing   = "missing"
	replayStateQueued    = "queued"
	replayStateParsing   = "parsing"
	replayStateReady     = "ready"
	replayStateFailed    = "failed"
	defaultWebhookHeader = "X-Webhook-Secret"
	defaultReplaysDir    = "./parsed"
	defaultStateFileName = "replay_state.json"
	maxWebhookBodyBytes  = 1 << 20
)

type replayWork struct {
	EventID   string
	MatchID   string
	MapID     string
	MatchInstanceID string
	DemoURL   string
	Timestamp time.Time
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
	mu                sync.RWMutex
	records           map[string]*replayRecord
	latestMapByMatch  map[string]string
	seenEvents        map[string]struct{}
	queueOrder        []string
	queue             chan replayWork
	replaysDir        string
	stateFile         string
	persistMu         sync.Mutex
	webhookHeaderName string
	webhookHeaderVal  string
	adminToken        string
	httpClient        *http.Client
}

type replayPersistentState struct {
	Version          int                     `json:"version"`
	Records          map[string]replayRecord `json:"records"`
	LatestMapByMatch map[string]string       `json:"latest_map_by_match"`
	SeenEvents       []string                `json:"seen_events"`
	QueueOrder       []string                `json:"queue_order"`
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

	svc := &replayService{
		records:           map[string]*replayRecord{},
		latestMapByMatch:  map[string]string{},
		seenEvents:        map[string]struct{}{},
		queueOrder:        make([]string, 0),
		queue:             make(chan replayWork, queueSize),
		replaysDir:        replaysDir,
		stateFile:         stateFile,
		webhookHeaderName: headerName,
		webhookHeaderVal:  os.Getenv("WEBHOOK_HEADER_VALUE"),
		adminToken:        os.Getenv("ADMIN_REPROCESS_TOKEN"),
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
	return svc, nil
}

func eventDedupeKey(eventID string, matchID string, mapID string) string {
	return eventID + "::" + matchID + "::" + mapID
}

func (s *replayService) startWorkers(n int) {
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		go s.workerLoop(i + 1)
	}
	go s.recoverPendingJobs()
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
	for key, rec := range persisted.Records {
		recCopy := rec
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

	s.mu.Lock()
	s.records = records
	s.latestMapByMatch = latest
	s.seenEvents = seen
	s.queueOrder = queueOrder
	s.mu.Unlock()

	logger.Info("loaded replay state", zap.Int("records", len(records)), zap.Int("seen_events", len(seen)))
	return nil
}

func (s *replayService) persistState() error {
	s.mu.RLock()
	persisted := replayPersistentState{
		Version:          1,
		Records:          make(map[string]replayRecord, len(s.records)),
		LatestMapByMatch: make(map[string]string, len(s.latestMapByMatch)),
		SeenEvents:       make([]string, 0, len(s.seenEvents)),
		QueueOrder:       make([]string, 0, len(s.queueOrder)),
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
		s.mu.Unlock()
		s.updateState(job.MatchID, job.MapID, replayStateParsing, "", "", "", job.EventID)

		if err := s.process(job); err != nil {
			logger.Error("replay processing failed",
				zap.String("match_id", job.MatchID),
				zap.String("map_id", job.MapID),
				zap.String("event_id", job.EventID),
				zap.Error(err))
			s.updateState(job.MatchID, job.MapID, replayStateFailed, "", err.Error(), "", job.EventID)
			continue
		}
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

func (s *replayService) process(job replayWork) error {
	secureURL, err := secureDemoUrl(job.DemoURL, isDev)
	if err != nil {
		return fmt.Errorf("invalid demo url: %w", err)
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
	if err := tmpFile.Sync(); err != nil {
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

	parseErr := pparser.WasmParseDemo(filepath.Base(secureURL), tmpFile, func(payload []byte) {
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
	if ok {
		queuePosition, queueTotal = s.queuePositionLocked(rec.MatchID, rec.MapID, rec.State)
	}
	s.mu.RUnlock()
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
		"match_id":            rec.MatchID,
		"map_id":              rec.MapID,
		"match_instance_id":   rec.MatchInstanceID,
		"state":               rec.State,
		"artifact_path":       rec.ArtifactPath,
		"demo_url":            rec.DemoURL,
		"last_error":          rec.LastError,
		"last_event_id":       rec.LastEventID,
		"last_transaction_id": rec.LastTransactionID,
		"last_updated_at":     rec.LastUpdatedAt,
		"created_at":          rec.CreatedAt,
		"last_webhook_event_at": rec.LastWebhookEventAt,
		"map_ids":             mapIDs,
	}
	if rec.State == replayStateQueued || rec.State == replayStateParsing {
		if queuePosition >= 0 {
			resp["queue_position"] = queuePosition
		}
		resp["queue_total"] = queueTotal
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *replayService) handleReplayFetch(w http.ResponseWriter, r *http.Request, matchID string, mapID string) {
	s.mu.RLock()
	rec, ok := s.getLocked(matchID, mapID)
	s.mu.RUnlock()
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
		}
		writeJSON(w, http.StatusAccepted, resp)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Replay-State", rec.State)
	w.Header().Set("X-Replay-Map-Id", rec.MapID)
	w.Header().Set("Cache-Control", "public, max-age=300")
	http.ServeFile(w, r, rec.ArtifactPath)
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

	s.updateState(req.MatchID, mapID, replayStateQueued, "", "", req.DemoURL, "manual")
	select {
	case s.queue <- replayWork{
		EventID:   "manual-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		MatchID:   req.MatchID,
		MapID:     mapID,
		DemoURL:   req.DemoURL,
		Timestamp: time.Now().UTC(),
	}:
		writeJSON(w, http.StatusAccepted, map[string]string{
			"match_id": req.MatchID,
			"map_id":   mapID,
			"state":    replayStateQueued,
		})
	default:
		http.Error(w, "ingest queue is full", http.StatusServiceUnavailable)
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
