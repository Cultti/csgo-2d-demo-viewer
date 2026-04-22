package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestValidateFaceitDemoReady(t *testing.T) {
	valid := faceitDemoReadyEvent{}
	valid.Event = "match_demo_ready"
	valid.EventID = "evt-1"
	valid.Payload.ID = "1-72891900-e990-4f3a-a73c-2fe2b4f43abb"
	valid.Payload.Game = "cs2"
	valid.Payload.DemoURL = "https://demos-europe-central.backblaze.faceit-cdn.net/cs2/1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2-1.dem.zst"
	valid.Payload.MatchInstanceID = "1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2-1"

	if err := validateFaceitDemoReady(valid); err != nil {
		t.Fatalf("expected valid payload, got error: %v", err)
	}

	invalid := valid
	invalid.Payload.Game = "dota2"
	if err := validateFaceitDemoReady(invalid); err == nil {
		t.Fatalf("expected invalid game to fail")
	}
}

func TestWebhookRejectsWrongSecret(t *testing.T) {
	t.Setenv("WEBHOOK_HEADER_NAME", "X-Test-Secret")
	t.Setenv("WEBHOOK_HEADER_VALUE", "secret-value")

	tempDir := t.TempDir()
	t.Setenv("REPLAYS_DIR", tempDir)
	logger = zap.NewNop()

	svc, err := newReplayService()
	if err != nil {
		t.Fatalf("newReplayService failed: %v", err)
	}

	payload := faceitDemoReadyEvent{}
	payload.Event = "match_demo_ready"
	payload.EventID = "evt-2"
	payload.Payload.ID = "1-72891900-e990-4f3a-a73c-2fe2b4f43abb"
	payload.Payload.Game = "cs2"
	payload.Payload.DemoURL = "https://demos-europe-central.backblaze.faceit-cdn.net/cs2/1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2-1.dem.zst"
	payload.Payload.MatchInstanceID = "1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2-1"

	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/faceit/demo-ready", bytes.NewReader(body))
	req.Header.Set("X-Test-Secret", "wrong")
	rr := httptest.NewRecorder()

	svc.webhookDemoReadyHandler(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestWebhookQueuesReplay(t *testing.T) {
	t.Setenv("WEBHOOK_HEADER_NAME", "X-Test-Secret")
	t.Setenv("WEBHOOK_HEADER_VALUE", "secret-value")
	t.Setenv("REPLAYS_DIR", t.TempDir())
	logger = zap.NewNop()

	svc, err := newReplayService()
	if err != nil {
		t.Fatalf("newReplayService failed: %v", err)
	}

	payload := faceitDemoReadyEvent{}
	payload.Event = "match_demo_ready"
	payload.EventID = "evt-3"
	payload.Payload.ID = "1-72891900-e990-4f3a-a73c-2fe2b4f43abb"
	payload.Payload.Game = "cs2"
	payload.Payload.DemoURL = "https://demos-europe-central.backblaze.faceit-cdn.net/cs2/1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2-1.dem.zst"
	payload.Payload.MatchInstanceID = "1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2-1"

	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/faceit/demo-ready", bytes.NewReader(body))
	req.Header.Set("X-Test-Secret", "secret-value")
	rr := httptest.NewRecorder()

	svc.webhookDemoReadyHandler(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rr.Code, rr.Body.String())
	}

	svc.mu.RLock()
	rec, ok := svc.records[replayRecordKey(payload.Payload.ID, "2")]
	svc.mu.RUnlock()
	if !ok {
		t.Fatalf("expected replay record to exist")
	}
	if rec.State != replayStateQueued {
		t.Fatalf("expected queued state, got %s", rec.State)
	}
}

func TestWebhookDuplicateEventIsAcceptedAndNotRequeued(t *testing.T) {
	t.Setenv("WEBHOOK_HEADER_NAME", "X-Test-Secret")
	t.Setenv("WEBHOOK_HEADER_VALUE", "secret-value")
	t.Setenv("REPLAYS_DIR", t.TempDir())
	logger = zap.NewNop()

	svc, err := newReplayService()
	if err != nil {
		t.Fatalf("newReplayService failed: %v", err)
	}

	payload := faceitDemoReadyEvent{}
	payload.Event = "match_demo_ready"
	payload.EventID = "evt-duplicate"
	payload.Payload.ID = "1-72891900-e990-4f3a-a73c-2fe2b4f43abb"
	payload.Payload.Game = "cs2"
	payload.Payload.DemoURL = "https://demos-europe-central.backblaze.faceit-cdn.net/cs2/1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2-1.dem.zst"
	payload.Payload.MatchInstanceID = "1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2-1"

	body, _ := json.Marshal(payload)
	req1 := httptest.NewRequest(http.MethodPost, "/webhooks/faceit/demo-ready", bytes.NewReader(body))
	req1.Header.Set("X-Test-Secret", "secret-value")
	rr1 := httptest.NewRecorder()
	svc.webhookDemoReadyHandler(rr1, req1)
	if rr1.Code != http.StatusAccepted {
		t.Fatalf("expected first webhook to be accepted, got %d", rr1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/webhooks/faceit/demo-ready", bytes.NewReader(body))
	req2.Header.Set("X-Test-Secret", "secret-value")
	rr2 := httptest.NewRecorder()
	svc.webhookDemoReadyHandler(rr2, req2)
	if rr2.Code != http.StatusAccepted {
		t.Fatalf("expected duplicate webhook to be accepted, got %d", rr2.Code)
	}

	if qLen := len(svc.queue); qLen != 1 {
		t.Fatalf("expected single queued job after duplicate events, got %d", qLen)
	}
}

func TestWebhookReadyReplayIsNotRequeued(t *testing.T) {
	t.Setenv("WEBHOOK_HEADER_NAME", "X-Test-Secret")
	t.Setenv("WEBHOOK_HEADER_VALUE", "secret-value")
	t.Setenv("REPLAYS_DIR", t.TempDir())
	logger = zap.NewNop()

	svc, err := newReplayService()
	if err != nil {
		t.Fatalf("newReplayService failed: %v", err)
	}

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.records[replayRecordKey("1-72891900-e990-4f3a-a73c-2fe2b4f43abb", "2")] = &replayRecord{
		MatchID:       "1-72891900-e990-4f3a-a73c-2fe2b4f43abb",
		MapID:         "2",
		State:         replayStateReady,
		ArtifactPath:  "/tmp/already-ready.pbr.gz",
		CreatedAt:     now,
		LastUpdatedAt: now,
	}
	svc.latestMapByMatch["1-72891900-e990-4f3a-a73c-2fe2b4f43abb"] = "2"
	svc.mu.Unlock()

	payload := faceitDemoReadyEvent{}
	payload.Event = "match_demo_ready"
	payload.EventID = "evt-new-for-ready"
	payload.Payload.ID = "1-72891900-e990-4f3a-a73c-2fe2b4f43abb"
	payload.Payload.Game = "cs2"
	payload.Payload.Round = 2
	payload.Payload.DemoURL = "https://pappa.aukko.net/demos/x/1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2.dem.zst"
	payload.Payload.MatchInstanceID = "1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2-1"

	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/faceit/demo-ready", bytes.NewReader(body))
	req.Header.Set("X-Test-Secret", "secret-value")
	rr := httptest.NewRecorder()
	svc.webhookDemoReadyHandler(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected webhook to be accepted for already-ready replay, got %d", rr.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["result"] != "already_ready" {
		t.Fatalf("expected result already_ready, got %q", resp["result"])
	}

	if qLen := len(svc.queue); qLen != 0 {
		t.Fatalf("expected no new queued jobs for already-ready replay, got %d", qLen)
	}
}

func TestWebhookSameEventIDDifferentMapIsNotDeduped(t *testing.T) {
	t.Setenv("WEBHOOK_HEADER_NAME", "X-Test-Secret")
	t.Setenv("WEBHOOK_HEADER_VALUE", "secret-value")
	t.Setenv("REPLAYS_DIR", t.TempDir())
	logger = zap.NewNop()

	svc, err := newReplayService()
	if err != nil {
		t.Fatalf("newReplayService failed: %v", err)
	}

	event1 := faceitDemoReadyEvent{}
	event1.Event = "match_demo_ready"
	event1.EventID = "evt-shared"
	event1.Payload.ID = "1-b6f50ed3-2da2-4379-bc8b-19e18874f509"
	event1.Payload.Game = "cs2"
	event1.Payload.Round = 1
	event1.Payload.DemoURL = "https://pappa.aukko.net/demos/x/1-b6f50ed3-2da2-4379-bc8b-19e18874f509-1.dem.zst"
	event1.Payload.MatchInstanceID = "1-b6f50ed3-2da2-4379-bc8b-19e18874f509-1-1"

	body1, _ := json.Marshal(event1)
	req1 := httptest.NewRequest(http.MethodPost, "/webhooks/faceit/demo-ready", bytes.NewReader(body1))
	req1.Header.Set("X-Test-Secret", "secret-value")
	rr1 := httptest.NewRecorder()
	svc.webhookDemoReadyHandler(rr1, req1)
	if rr1.Code != http.StatusAccepted {
		t.Fatalf("expected first webhook to be accepted, got %d", rr1.Code)
	}

	event2 := event1
	event2.Payload.Round = 2
	event2.Payload.DemoURL = "https://pappa.aukko.net/demos/x/1-b6f50ed3-2da2-4379-bc8b-19e18874f509-2.dem.zst"
	event2.Payload.MatchInstanceID = "1-b6f50ed3-2da2-4379-bc8b-19e18874f509-2-1"
	body2, _ := json.Marshal(event2)
	req2 := httptest.NewRequest(http.MethodPost, "/webhooks/faceit/demo-ready", bytes.NewReader(body2))
	req2.Header.Set("X-Test-Secret", "secret-value")
	rr2 := httptest.NewRecorder()
	svc.webhookDemoReadyHandler(rr2, req2)
	if rr2.Code != http.StatusAccepted {
		t.Fatalf("expected second map webhook to be accepted, got %d", rr2.Code)
	}

	if qLen := len(svc.queue); qLen != 2 {
		t.Fatalf("expected two queued jobs for different maps, got %d", qLen)
	}
}

func TestWebhookQueueFullDoesNotOverrideAlreadyQueuedState(t *testing.T) {
	t.Setenv("WEBHOOK_HEADER_NAME", "X-Test-Secret")
	t.Setenv("WEBHOOK_HEADER_VALUE", "secret-value")
	t.Setenv("REPLAYS_DIR", t.TempDir())
	logger = zap.NewNop()

	svc, err := newReplayService()
	if err != nil {
		t.Fatalf("newReplayService failed: %v", err)
	}
	svc.queue = make(chan replayWork, 1)

	first := faceitDemoReadyEvent{}
	first.Event = "match_demo_ready"
	first.EventID = "evt-first"
	first.Payload.ID = "1-b6f50ed3-2da2-4379-bc8b-19e18874f509"
	first.Payload.Game = "cs2"
	first.Payload.Round = 1
	first.Payload.DemoURL = "https://pappa.aukko.net/demos/x/1-b6f50ed3-2da2-4379-bc8b-19e18874f509-1.dem.zst"
	first.Payload.MatchInstanceID = "1-b6f50ed3-2da2-4379-bc8b-19e18874f509-1-1"

	body1, _ := json.Marshal(first)
	req1 := httptest.NewRequest(http.MethodPost, "/webhooks/faceit/demo-ready", bytes.NewReader(body1))
	req1.Header.Set("X-Test-Secret", "secret-value")
	rr1 := httptest.NewRecorder()
	svc.webhookDemoReadyHandler(rr1, req1)
	if rr1.Code != http.StatusAccepted {
		t.Fatalf("expected first webhook to be accepted, got %d", rr1.Code)
	}

	second := first
	second.EventID = "evt-second"
	body2, _ := json.Marshal(second)
	req2 := httptest.NewRequest(http.MethodPost, "/webhooks/faceit/demo-ready", bytes.NewReader(body2))
	req2.Header.Set("X-Test-Secret", "secret-value")
	rr2 := httptest.NewRecorder()
	svc.webhookDemoReadyHandler(rr2, req2)
	if rr2.Code != http.StatusAccepted {
		t.Fatalf("expected second webhook to be accepted for already queued replay, got %d", rr2.Code)
	}

	svc.mu.RLock()
	rec, ok := svc.records[replayRecordKey(first.Payload.ID, "1")]
	svc.mu.RUnlock()
	if !ok {
		t.Fatalf("expected replay record to exist")
	}
	if rec.State != replayStateQueued {
		t.Fatalf("expected queued state, got %s", rec.State)
	}
	if rec.LastError != "" {
		t.Fatalf("expected empty last_error for queued replay, got %q", rec.LastError)
	}
}

func TestReplayStatePersistenceRoundTrip(t *testing.T) {
	t.Setenv("REPLAYS_DIR", t.TempDir())
	logger = zap.NewNop()

	svc, err := newReplayService()
	if err != nil {
		t.Fatalf("newReplayService failed: %v", err)
	}

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.records[replayRecordKey("demo-1", "2")] = &replayRecord{
		MatchID:            "demo-1",
		MapID:              "2",
		MatchInstanceID:    "demo-1-2-1",
		State:              replayStateQueued,
		DemoURL:            "https://pappa.aukko.net/demos/x/demo-1-2.dem.zst",
		LastEventID:        "evt-1",
		LastUpdatedAt:      now,
		CreatedAt:          now,
		LastWebhookEventAt: now,
	}
	svc.latestMapByMatch["demo-1"] = "2"
	svc.seenEvents["evt-1::demo-1::2"] = struct{}{}
	svc.mu.Unlock()

	if err := svc.persistState(); err != nil {
		t.Fatalf("persistState failed: %v", err)
	}

	reloaded, err := newReplayService()
	if err != nil {
		t.Fatalf("newReplayService reload failed: %v", err)
	}

	reloaded.mu.RLock()
	rec, ok := reloaded.records[replayRecordKey("demo-1", "2")]
	_, seen := reloaded.seenEvents["evt-1::demo-1::2"]
	latest := reloaded.latestMapByMatch["demo-1"]
	reloaded.mu.RUnlock()

	if !ok {
		t.Fatalf("expected record to be loaded from persisted state")
	}
	if rec.State != replayStateQueued {
		t.Fatalf("expected queued state after reload, got %s", rec.State)
	}
	if rec.DemoURL == "" {
		t.Fatalf("expected demo_url to survive reload")
	}
	if latest != "2" {
		t.Fatalf("expected latest map 2 after reload, got %s", latest)
	}
	if !seen {
		t.Fatalf("expected seen event to survive reload")
	}
}

func TestRecoverPendingJobsRequeuesQueuedAndParsing(t *testing.T) {
	t.Setenv("REPLAYS_DIR", t.TempDir())
	t.Setenv("QUEUE_SIZE", "10")
	logger = zap.NewNop()

	svc, err := newReplayService()
	if err != nil {
		t.Fatalf("newReplayService failed: %v", err)
	}

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.records[replayRecordKey("demo-1", "1")] = &replayRecord{
		MatchID:            "demo-1",
		MapID:              "1",
		State:              replayStateQueued,
		DemoURL:            "https://pappa.aukko.net/demos/x/demo-1-1.dem.zst",
		LastEventID:        "evt-1",
		LastWebhookEventAt: now,
		CreatedAt:          now,
		LastUpdatedAt:      now,
	}
	svc.records[replayRecordKey("demo-1", "2")] = &replayRecord{
		MatchID:            "demo-1",
		MapID:              "2",
		State:              replayStateParsing,
		DemoURL:            "https://pappa.aukko.net/demos/x/demo-1-2.dem.zst",
		LastEventID:        "evt-2",
		LastWebhookEventAt: now,
		CreatedAt:          now,
		LastUpdatedAt:      now,
	}
	svc.mu.Unlock()

	svc.recoverPendingJobs()

	if qLen := len(svc.queue); qLen != 2 {
		t.Fatalf("expected two recovered jobs in queue, got %d", qLen)
	}

	svc.mu.RLock()
	rec := svc.records[replayRecordKey("demo-1", "2")]
	svc.mu.RUnlock()
	if rec.State != replayStateQueued {
		t.Fatalf("expected parsing record to be reset to queued, got %s", rec.State)
	}
}

func TestMapIDFromMatchInstance(t *testing.T) {
	matchID := "1-72891900-e990-4f3a-a73c-2fe2b4f43abb"
	instanceID := "1-72891900-e990-4f3a-a73c-2fe2b4f43abb-2-1"
	got := mapIDFromMatchInstance(matchID, instanceID, 0)
	if got != "2" {
		t.Fatalf("expected map id 2, got %s", got)
	}

	fallback := mapIDFromMatchInstance(matchID, "", 3)
	if fallback != "3" {
		t.Fatalf("expected fallback round 3, got %s", fallback)
	}
}

func TestMapIDFromDemoURL(t *testing.T) {
	urlSingle := "https://pappa.aukko.net/demos/xxx/1-b6f50ed3-2da2-4379-bc8b-19e18874f509-2.dem.zst"
	if got := mapIDFromDemoURL(urlSingle); got != "2" {
		t.Fatalf("expected map id 2 from single suffix url, got %s", got)
	}

	urlDouble := "https://host/cs2/1-b6f50ed3-2da2-4379-bc8b-19e18874f509-2-1.dem.zst"
	if got := mapIDFromDemoURL(urlDouble); got != "2" {
		t.Fatalf("expected map id 2 from double suffix url, got %s", got)
	}
}

func TestMapIDFromEventPrefersPayloadRound(t *testing.T) {
	event := faceitDemoReadyEvent{}
	event.Payload.ID = "1-b6f50ed3-2da2-4379-bc8b-19e18874f509"
	event.Payload.Round = 3
	event.Payload.MatchInstanceID = "1-b6f50ed3-2da2-4379-bc8b-19e18874f509-2-1"
	event.Payload.DemoURL = "https://pappa.aukko.net/demos/xxx/1-b6f50ed3-2da2-4379-bc8b-19e18874f509-2.dem.zst"

	if got := mapIDFromEvent(event); got != "3" {
		t.Fatalf("expected round-derived map id 3, got %s", got)
	}
}

func TestReplayStatusAndFetchHandlers(t *testing.T) {
	logger = zap.NewNop()
	svc := &replayService{
		records:          map[string]*replayRecord{},
		latestMapByMatch: map[string]string{},
		seenEvents:       map[string]struct{}{},
		queue:            make(chan replayWork, 1),
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/replays/demo-1/status", nil)
	statusRes := httptest.NewRecorder()
	svc.replaysHandler(statusRes, statusReq)
	if statusRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing replay, got %d", statusRes.Code)
	}

	tempDir := t.TempDir()
	artifact := filepath.Join(tempDir, "artifact.dem.zst")
	if err := os.WriteFile(artifact, []byte("demo-bytes"), 0o644); err != nil {
		t.Fatalf("failed to create artifact: %v", err)
	}

	svc.records[replayRecordKey("demo-1", "2")] = &replayRecord{
		MatchID:      "demo-1",
		MapID:        "2",
		State:        replayStateReady,
		ArtifactPath: artifact,
	}
	svc.latestMapByMatch = map[string]string{"demo-1": "2"}

	fetchReq := httptest.NewRequest(http.MethodGet, "/replays/demo-1", nil)
	fetchRes := httptest.NewRecorder()
	svc.replaysHandler(fetchRes, fetchReq)
	if fetchRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for ready replay, got %d", fetchRes.Code)
	}
	if fetchRes.Body.String() != "demo-bytes" {
		t.Fatalf("unexpected artifact body: %q", fetchRes.Body.String())
	}
}

func TestReplayMapIDQuerySelection(t *testing.T) {
	logger = zap.NewNop()
	svc := &replayService{
		records:          map[string]*replayRecord{},
		latestMapByMatch: map[string]string{},
		seenEvents:       map[string]struct{}{},
		queue:            make(chan replayWork, 1),
	}

	tempDir := t.TempDir()
	artifact1 := filepath.Join(tempDir, "artifact-map1.pbr.gz")
	artifact2 := filepath.Join(tempDir, "artifact-map2.pbr.gz")
	if err := os.WriteFile(artifact1, []byte("map1"), 0o644); err != nil {
		t.Fatalf("failed to create artifact1: %v", err)
	}
	if err := os.WriteFile(artifact2, []byte("map2"), 0o644); err != nil {
		t.Fatalf("failed to create artifact2: %v", err)
	}

	svc.records[replayRecordKey("demo-1", "1")] = &replayRecord{MatchID: "demo-1", MapID: "1", State: replayStateReady, ArtifactPath: artifact1}
	svc.records[replayRecordKey("demo-1", "2")] = &replayRecord{MatchID: "demo-1", MapID: "2", State: replayStateReady, ArtifactPath: artifact2}
	svc.latestMapByMatch["demo-1"] = "2"

	fetchReq := httptest.NewRequest(http.MethodGet, "/replays/demo-1?map_id=1", nil)
	fetchRes := httptest.NewRecorder()
	svc.replaysHandler(fetchRes, fetchReq)
	if fetchRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for ready replay, got %d", fetchRes.Code)
	}
	if fetchRes.Body.String() != "map1" {
		t.Fatalf("expected map1 artifact, got %q", fetchRes.Body.String())
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/replays/demo-1/status?map_id=2", nil)
	statusRes := httptest.NewRecorder()
	svc.replaysHandler(statusRes, statusReq)
	if statusRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for map2 status, got %d", statusRes.Code)
	}
}

func TestReplayStatusIncludesQueuePosition(t *testing.T) {
	logger = zap.NewNop()
	svc := &replayService{
		records:          map[string]*replayRecord{},
		latestMapByMatch: map[string]string{},
		seenEvents:       map[string]struct{}{},
		queueOrder:       []string{replayRecordKey("demo-1", "1"), replayRecordKey("demo-1", "2"), replayRecordKey("demo-1", "3")},
		queue:            make(chan replayWork, 3),
	}

	now := time.Now().UTC()
	svc.records[replayRecordKey("demo-1", "2")] = &replayRecord{
		MatchID:       "demo-1",
		MapID:         "2",
		State:         replayStateQueued,
		CreatedAt:     now,
		LastUpdatedAt: now,
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/replays/demo-1/status?map_id=2", nil)
	statusRes := httptest.NewRecorder()
	svc.replaysHandler(statusRes, statusReq)
	if statusRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for status, got %d", statusRes.Code)
	}

	var payload map[string]any
	if err := json.Unmarshal(statusRes.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode status json: %v", err)
	}
	if got := int(payload["queue_position"].(float64)); got != 2 {
		t.Fatalf("expected queue_position 2, got %d", got)
	}
	if got := int(payload["queue_total"].(float64)); got != 3 {
		t.Fatalf("expected queue_total 3, got %d", got)
	}
}
