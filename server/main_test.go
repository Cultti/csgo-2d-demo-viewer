package main

import (
	"testing"

	"go.uber.org/zap"
)

func TestMatchId(t *testing.T) {
	logger = zap.NewNop()

	testMatch := func(input string, expected string) {
		matched := extractMatchId(input)
		if matched != expected {
			t.Logf("%s didn't match expected %s", matched, expected)
			t.Fail()
		}
	}

	expected := "1-e9789885-ebda-4f07-90de-8e38d73e174b-1-1"

	testMatch("/cs2/1-e9789885-ebda-4f07-90de-8e38d73e174b-1-1.dem.zst?fshquiojfqos", expected)
	testMatch("/cs2/1-e9789885-ebda-4f07-90de-8e38d73e174b-1-1.dem.zst", expected)
	testMatch("/cs2/12-e9789885-ebda-4f07-90de-8e38d73e174b-1-12.dem.zst", "")
}

func TestSecureDemoURL(t *testing.T) {
	logger = zap.NewNop()

	faceitURL := "https://demos-europe-central.backblaze.faceit-cdn.net/cs2/1-e9789885-ebda-4f07-90de-8e38d73e174b-1-1.dem.zst"
	if _, err := secureDemoUrl(faceitURL, false); err != nil {
		t.Fatalf("expected faceit URL to be allowed in production mode: %v", err)
	}

	if _, err := secureDemoUrl(faceitURL, true); err != nil {
		t.Fatalf("expected faceit URL to be allowed in dev mode for webhook ingest: %v", err)
	}

	if _, err := secureDemoUrl("http://localhost:8080/testdemos/example.dem.zst", true); err != nil {
		t.Fatalf("expected localhost URL to be allowed in dev mode: %v", err)
	}

	if _, err := secureDemoUrl("http://localhost:8080/testdemos/example.dem.zst", false); err == nil {
		t.Fatalf("expected localhost URL to be rejected in production mode")
	}

	debugURL := "https://pappa.aukko.net/demos/4782a888-39d0-485e-a581-3d6c8c46b9be/1-b6f50ed3-2da2-4379-bc8b-19e18874f509-1.dem.zst"
	if _, err := secureDemoUrl(debugURL, true); err != nil {
		t.Fatalf("expected pappa.aukko.net debug URL to be allowed in dev mode: %v", err)
	}
	if _, err := secureDemoUrl(debugURL, false); err != nil {
		t.Fatalf("expected pappa.aukko.net debug URL to be allowed in production mode: %v", err)
	}
}
