package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestParseCSVList(t *testing.T) {
	keys := parseCSVList("key-a, key-b ,,key-c")
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if keys[0] != "key-a" || keys[1] != "key-b" || keys[2] != "key-c" {
		t.Fatalf("unexpected parsed keys: %#v", keys)
	}
}

func TestResolveDemoDownloadURLUsesFaceitDownloadsAPIWithKeyFallback(t *testing.T) {
	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			t.Fatalf("expected Bearer auth header")
		}

		if r.Header.Get("Authorization") == "Bearer bad-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"payload":{"download_url":"https://signed.example.com/demo.dem.zst?sig=ok"}}`))
	}))
	defer ts.Close()

	svc := &replayService{
		faceitDownloadAPIURL:  ts.URL,
		faceitDownloadAPIKeys: []string{"bad-key", "good-key"},
		httpClient:            ts.Client(),
	}

	resolved, err := svc.resolveDemoDownloadURL("https://demos.faceit.com/cs2/1-cb038819-b0d0-4471-b25c-0e7468ab1eb1-1-1.dem.gz")
	if err != nil {
		t.Fatalf("expected faceit url to resolve via downloads api, got err: %v", err)
	}
	if !strings.HasPrefix(resolved, "https://signed.example.com/demo.dem.zst") {
		t.Fatalf("unexpected resolved signed url: %s", resolved)
	}
	if requestCount != 2 {
		t.Fatalf("expected two requests (key fallback), got %d", requestCount)
	}
}

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
		workerCount:      1,
		processingDurations: []time.Duration{time.Minute},
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
	if got := int(payload["eta_minutes"].(float64)); got != 2 {
		t.Fatalf("expected eta_minutes 2, got %d", got)
	}
}

func TestReplayFetchPendingIncludesQueueETA(t *testing.T) {
	logger = zap.NewNop()
	now := time.Now().UTC()
	svc := &replayService{
		records: map[string]*replayRecord{
			replayRecordKey("demo-1", "2"): {
				MatchID:       "demo-1",
				MapID:         "2",
				State:         replayStateQueued,
				CreatedAt:     now,
				LastUpdatedAt: now,
			},
		},
		latestMapByMatch: map[string]string{"demo-1": "2"},
		seenEvents:       map[string]struct{}{},
		queueOrder:       []string{replayRecordKey("demo-1", "2")},
		activeParses:     1,
		workerCount:      1,
		processingDurations: []time.Duration{2 * time.Minute},
		queue:            make(chan replayWork, 2),
	}

	req := httptest.NewRequest(http.MethodGet, "/replays/demo-1?map_id=2", nil)
	rr := httptest.NewRecorder()
	svc.replaysHandler(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for queued replay fetch, got %d", rr.Code)
	}

	var payload map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode fetch json: %v", err)
	}
	if got := int(payload["queue_position"].(float64)); got != 1 {
		t.Fatalf("expected queue_position 1, got %d", got)
	}
	if got := int(payload["queue_total"].(float64)); got != 1 {
		t.Fatalf("expected queue_total 1, got %d", got)
	}
	if got := int(payload["eta_minutes"].(float64)); got != 4 {
		t.Fatalf("expected eta_minutes 4, got %d", got)
	}
}

func TestReplayStatusRecoversFromArtifactsOnDisk(t *testing.T) {
	logger = zap.NewNop()
	replaysDir := t.TempDir()
	matchID := "1-5000aa78-482d-4032-a354-64cb1ded384d"
	matchDir := filepath.Join(replaysDir, matchID)
	if err := os.MkdirAll(matchDir, 0o755); err != nil {
		t.Fatalf("failed to create match directory: %v", err)
	}
	artifactPath := filepath.Join(matchDir, matchID+"-1.pbr.gz")
	if err := os.WriteFile(artifactPath, []byte("artifact"), 0o644); err != nil {
		t.Fatalf("failed to create artifact: %v", err)
	}

	svc := &replayService{
		records:          map[string]*replayRecord{},
		latestMapByMatch: map[string]string{},
		seenEvents:       map[string]struct{}{},
		queueOrder:       []string{},
		queue:            make(chan replayWork, 1),
		replaysDir:        replaysDir,
		stateFile:         filepath.Join(replaysDir, "replay_state.json"),
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/replays/"+matchID+"/status?map_id=1", nil)
	statusRes := httptest.NewRecorder()
	svc.replaysHandler(statusRes, statusReq)
	if statusRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for recovered replay, got %d body=%s", statusRes.Code, statusRes.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(statusRes.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode status json: %v", err)
	}
	if payload["state"] != replayStateReady {
		t.Fatalf("expected ready state after disk recovery, got %v", payload["state"])
	}
}

func TestReplayStatusAndFetchMissingWhenRequestedMapMissing(t *testing.T) {
	logger = zap.NewNop()
	replaysDir := t.TempDir()
	matchID := "1-5000aa78-482d-4032-a354-64cb1ded384d"
	matchDir := filepath.Join(replaysDir, matchID)
	if err := os.MkdirAll(matchDir, 0o755); err != nil {
		t.Fatalf("failed to create match directory: %v", err)
	}
	artifactMap1 := filepath.Join(matchDir, matchID+"-1.pbr.gz")
	artifactMap2 := filepath.Join(matchDir, matchID+"-2.pbr.gz")
	if err := os.WriteFile(artifactMap1, []byte("map1"), 0o644); err != nil {
		t.Fatalf("failed to create map1 artifact: %v", err)
	}
	if err := os.WriteFile(artifactMap2, []byte("map2"), 0o644); err != nil {
		t.Fatalf("failed to create map2 artifact: %v", err)
	}

	svc := &replayService{
		records:          map[string]*replayRecord{},
		latestMapByMatch: map[string]string{},
		seenEvents:       map[string]struct{}{},
		queueOrder:       []string{},
		queue:            make(chan replayWork, 1),
		replaysDir:        replaysDir,
		stateFile:         filepath.Join(replaysDir, "replay_state.json"),
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/replays/"+matchID+"/status?map_id=9", nil)
	statusRes := httptest.NewRecorder()
	svc.replaysHandler(statusRes, statusReq)
	if statusRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when requested map is missing, got %d body=%s", statusRes.Code, statusRes.Body.String())
	}

	var statusPayload map[string]any
	if err := json.Unmarshal(statusRes.Body.Bytes(), &statusPayload); err != nil {
		t.Fatalf("failed to decode status json: %v", err)
	}
	if statusPayload["state"] != replayStateMissing {
		t.Fatalf("expected missing state for missing requested map, got %v", statusPayload["state"])
	}
	if statusPayload["map_id"] != "9" {
		t.Fatalf("expected response map_id 9, got %v", statusPayload["map_id"])
	}

	fetchReq := httptest.NewRequest(http.MethodGet, "/replays/"+matchID+"?map_id=9", nil)
	fetchRes := httptest.NewRecorder()
	svc.replaysHandler(fetchRes, fetchReq)
	if fetchRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 fetch when requested map is missing, got %d body=%s", fetchRes.Code, fetchRes.Body.String())
	}
}

func TestLoadStateNormalizesReadyArtifactPath(t *testing.T) {
	logger = zap.NewNop()
	replaysDir := t.TempDir()
	t.Setenv("REPLAYS_DIR", replaysDir)

	matchID := "1-4d45d071-c885-4268-80c9-4a2f95eb3d2c"
	mapID := "2"
	matchDir := filepath.Join(replaysDir, matchID)
	if err := os.MkdirAll(matchDir, 0o755); err != nil {
		t.Fatalf("failed to create match directory: %v", err)
	}
	expectedArtifact := filepath.Join(matchDir, matchID+"-"+mapID+".pbr.gz")
	if err := os.WriteFile(expectedArtifact, []byte("artifact"), 0o644); err != nil {
		t.Fatalf("failed to create artifact: %v", err)
	}

	persisted := replayPersistentState{
		Version:          1,
		Records:          map[string]replayRecord{},
		LatestMapByMatch: map[string]string{matchID: mapID},
		SeenEvents:       []string{},
		QueueOrder:       []string{},
	}
	persisted.Records[replayRecordKey(matchID, mapID)] = replayRecord{
		MatchID:      matchID,
		MapID:        mapID,
		State:        replayStateReady,
		ArtifactPath: filepath.Join("parsed", matchID, matchID+"-"+mapID+".pbr.gz"),
	}
	stateBytes, err := json.Marshal(persisted)
	if err != nil {
		t.Fatalf("failed to marshal persisted state: %v", err)
	}
	stateFile := filepath.Join(replaysDir, "replay_state.json")
	if err := os.WriteFile(stateFile, stateBytes, 0o644); err != nil {
		t.Fatalf("failed to write persisted state: %v", err)
	}

	svc, err := newReplayService()
	if err != nil {
		t.Fatalf("newReplayService failed: %v", err)
	}

	svc.mu.RLock()
	rec, ok := svc.records[replayRecordKey(matchID, mapID)]
	svc.mu.RUnlock()
	if !ok {
		t.Fatalf("expected replay record to be loaded")
	}
	if rec.ArtifactPath != expectedArtifact {
		t.Fatalf("expected normalized artifact path %q, got %q", expectedArtifact, rec.ArtifactPath)
	}
}

func TestReplayFetchRecoversWhenRecordArtifactPathIsStale(t *testing.T) {
	logger = zap.NewNop()
	replaysDir := t.TempDir()
	matchID := "1-4d45d071-c885-4268-80c9-4a2f95eb3d2c"
	mapID := "2"
	matchDir := filepath.Join(replaysDir, matchID)
	if err := os.MkdirAll(matchDir, 0o755); err != nil {
		t.Fatalf("failed to create match directory: %v", err)
	}
	artifactPath := filepath.Join(matchDir, matchID+"-"+mapID+".pbr.gz")
	if err := os.WriteFile(artifactPath, []byte("recovered"), 0o644); err != nil {
		t.Fatalf("failed to create artifact: %v", err)
	}

	svc := &replayService{
		records: map[string]*replayRecord{
			replayRecordKey(matchID, mapID): {
				MatchID:      matchID,
				MapID:        mapID,
				State:        replayStateReady,
				ArtifactPath: filepath.Join("parsed", matchID, matchID+"-"+mapID+".pbr.gz"),
			},
		},
		latestMapByMatch: map[string]string{matchID: mapID},
		seenEvents:       map[string]struct{}{},
		queueOrder:       []string{},
		queue:            make(chan replayWork, 1),
		replaysDir:        replaysDir,
		stateFile:         filepath.Join(replaysDir, "replay_state.json"),
	}

	req := httptest.NewRequest(http.MethodGet, "/replays/"+matchID+"?map_id="+mapID, nil)
	rr := httptest.NewRecorder()
	svc.replaysHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 after stale path recovery, got %d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Body.String() != "recovered" {
		t.Fatalf("expected recovered artifact body, got %q", rr.Body.String())
	}
}

func TestCleanupStaleDownloadTempFiles(t *testing.T) {
	logger = zap.NewNop()
	replaysDir := t.TempDir()
	matchID := "1-e64dfef6-b61b-449e-8d98-f12ceefbba6c"
	matchDir := filepath.Join(replaysDir, matchID)
	if err := os.MkdirAll(matchDir, 0o755); err != nil {
		t.Fatalf("failed to create match directory: %v", err)
	}

	staleTemp := filepath.Join(matchDir, "download-1528820945.dem.zst")
	replayArtifact := filepath.Join(matchDir, matchID+"-1.pbr.gz")
	metaFile := filepath.Join(matchDir, "metadata.json")

	if err := os.WriteFile(staleTemp, []byte("tmp"), 0o644); err != nil {
		t.Fatalf("failed to create stale temp file: %v", err)
	}
	if err := os.WriteFile(replayArtifact, []byte("artifact"), 0o644); err != nil {
		t.Fatalf("failed to create replay artifact file: %v", err)
	}
	if err := os.WriteFile(metaFile, []byte(`{"ok":true}`), 0o644); err != nil {
		t.Fatalf("failed to create metadata file: %v", err)
	}

	svc := &replayService{replaysDir: replaysDir}
	svc.cleanupStaleDownloadTempFiles()

	if _, err := os.Stat(staleTemp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected stale temp file to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(replayArtifact); err != nil {
		t.Fatalf("expected replay artifact to be kept, stat err=%v", err)
	}
	if _, err := os.Stat(metaFile); err != nil {
		t.Fatalf("expected metadata file to be kept, stat err=%v", err)
	}
}

func TestAdminReprocessFailedRequiresAuth(t *testing.T) {
	logger = zap.NewNop()
	svc := &replayService{}

	req := httptest.NewRequest(http.MethodPost, "/admin/replays/reprocess-failed", bytes.NewReader([]byte(`{}`)))
	rr := httptest.NewRecorder()
	svc.adminReprocessFailedHandler(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without admin token, got %d", rr.Code)
	}
}

func TestAdminReprocessFailedDryRunAndQueue(t *testing.T) {
	logger = zap.NewNop()
	now := time.Now().UTC()
	svc := &replayService{
		records: map[string]*replayRecord{
			replayRecordKey("m-1", "1"): {
				MatchID:       "m-1",
				MapID:         "1",
				State:         replayStateFailed,
				DemoURL:       "https://pappa.aukko.net/demos/m-1-1.dem.zst",
				LastError:     "parse failed",
				LastUpdatedAt: now,
				CreatedAt:     now,
			},
			replayRecordKey("m-1", "2"): {
				MatchID:       "m-1",
				MapID:         "2",
				State:         replayStateFailed,
				DemoURL:       "",
				LastError:     "missing demo",
				LastUpdatedAt: now,
				CreatedAt:     now,
			},
			replayRecordKey("m-1", "3"): {
				MatchID:       "m-1",
				MapID:         "3",
				State:         replayStateReady,
				DemoURL:       "https://pappa.aukko.net/demos/m-1-3.dem.zst",
				LastUpdatedAt: now,
				CreatedAt:     now,
			},
			replayRecordKey("m-2", "1"): {
				MatchID:       "m-2",
				MapID:         "1",
				State:         replayStateFailed,
				DemoURL:       "https://pappa.aukko.net/demos/m-2-1.dem.zst",
				LastUpdatedAt: now,
				CreatedAt:     now,
			},
		},
		latestMapByMatch: map[string]string{"m-1": "3", "m-2": "1"},
		seenEvents:       map[string]struct{}{},
		queueOrder:       []string{},
		queue:            make(chan replayWork, 10),
		stateFile:        filepath.Join(t.TempDir(), "replay_state.json"),
		adminToken:       "token",
	}

	dryRunReq := httptest.NewRequest(http.MethodPost, "/admin/replays/reprocess-failed", bytes.NewReader([]byte(`{"match_id":"m-1","dry_run":true}`)))
	dryRunReq.Header.Set("X-Admin-Token", "token")
	dryRunRes := httptest.NewRecorder()
	svc.adminReprocessFailedHandler(dryRunRes, dryRunReq)
	if dryRunRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for dry run, got %d body=%s", dryRunRes.Code, dryRunRes.Body.String())
	}
	if qLen := len(svc.queue); qLen != 0 {
		t.Fatalf("expected no queue changes on dry run, got len=%d", qLen)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/replays/reprocess-failed", bytes.NewReader([]byte(`{"match_id":"m-1"}`)))
	req.Header.Set("X-Admin-Token", "token")
	res := httptest.NewRecorder()
	svc.adminReprocessFailedHandler(res, req)
	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for bulk reprocess, got %d body=%s", res.Code, res.Body.String())
	}
	if qLen := len(svc.queue); qLen != 1 {
		t.Fatalf("expected one queued failed replay with demo_url, got len=%d", qLen)
	}

	svc.mu.RLock()
	rec1 := svc.records[replayRecordKey("m-1", "1")]
	rec2 := svc.records[replayRecordKey("m-1", "2")]
	svc.mu.RUnlock()
	if rec1.State != replayStateQueued {
		t.Fatalf("expected m-1 map 1 to become queued, got %s", rec1.State)
	}
	if rec2.State != replayStateFailed {
		t.Fatalf("expected m-1 map 2 to remain failed (missing demo_url), got %s", rec2.State)
	}
}

func TestAdminDeleteReplayRequiresAuth(t *testing.T) {
	logger = zap.NewNop()
	svc := &replayService{}

	req := httptest.NewRequest(http.MethodPost, "/admin/replays/delete", bytes.NewReader([]byte(`{"match_id":"m-1","map_id":"1"}`)))
	rr := httptest.NewRecorder()
	svc.adminDeleteReplayHandler(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without admin token, got %d", rr.Code)
	}
}

func TestAdminDeleteReplayRemovesRecordAndIndexes(t *testing.T) {
	logger = zap.NewNop()
	now := time.Now().UTC()
	svc := &replayService{
		records: map[string]*replayRecord{
			replayRecordKey("m-1", "1"): {
				MatchID:       "m-1",
				MapID:         "1",
				State:         replayStateReady,
				LastUpdatedAt: now,
				CreatedAt:     now,
			},
			replayRecordKey("m-1", "2"): {
				MatchID:       "m-1",
				MapID:         "2",
				State:         replayStateQueued,
				LastUpdatedAt: now,
				CreatedAt:     now,
			},
			replayRecordKey("m-2", "1"): {
				MatchID:       "m-2",
				MapID:         "1",
				State:         replayStateReady,
				LastUpdatedAt: now,
				CreatedAt:     now,
			},
		},
		latestMapByMatch: map[string]string{"m-1": "2", "m-2": "1"},
		seenEvents: map[string]struct{}{
			"evt-a::m-1::2": {},
			"evt-b::m-1::1": {},
			"evt-c::m-2::1": {},
		},
		queueOrder: []string{replayRecordKey("m-1", "2"), replayRecordKey("m-2", "1")},
		queue:      make(chan replayWork, 10),
		stateFile:  filepath.Join(t.TempDir(), "replay_state.json"),
		adminToken: "token",
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/replays/delete", bytes.NewReader([]byte(`{"match_id":"m-1","map_id":"2"}`)))
	req.Header.Set("X-Admin-Token", "token")
	rr := httptest.NewRecorder()
	svc.adminDeleteReplayHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for admin delete, got %d body=%s", rr.Code, rr.Body.String())
	}

	svc.mu.RLock()
	_, hasDeletedRecord := svc.records[replayRecordKey("m-1", "2")]
	_, hasRemainingRecord := svc.records[replayRecordKey("m-1", "1")]
	_, hasDeletedSeen := svc.seenEvents["evt-a::m-1::2"]
	_, hasOtherSeen := svc.seenEvents["evt-b::m-1::1"]
	latestMapM1 := svc.latestMapByMatch["m-1"]
	queueOrder := append([]string(nil), svc.queueOrder...)
	svc.mu.RUnlock()

	if hasDeletedRecord {
		t.Fatalf("expected deleted replay record to be removed from state")
	}
	if !hasRemainingRecord {
		t.Fatalf("expected non-target replay record to remain")
	}
	if hasDeletedSeen {
		t.Fatalf("expected seen event key for deleted replay to be removed")
	}
	if !hasOtherSeen {
		t.Fatalf("expected seen event keys for other maps to remain")
	}
	if latestMapM1 != "1" {
		t.Fatalf("expected latest map for m-1 to fall back to map 1, got %s", latestMapM1)
	}
	if len(queueOrder) != 1 || queueOrder[0] != replayRecordKey("m-2", "1") {
		t.Fatalf("expected queue order to remove deleted key, got %#v", queueOrder)
	}

	data, err := os.ReadFile(svc.stateFile)
	if err != nil {
		t.Fatalf("expected persisted replay state file, got read err: %v", err)
	}
	if bytes.Contains(data, []byte(`"m-1::2"`)) {
		t.Fatalf("expected persisted state to exclude deleted replay key")
	}
}
