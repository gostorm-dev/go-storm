package storm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCollectResults(t *testing.T) {
	testCases := []struct {
		name            string
		results         []Result
		wantSuccessful  int
		wantFailed      int
		wantAvg         time.Duration
		wantStatusCodes map[int]int
	}{
		{
			name: "all success",
			results: []Result{
				{StatusCode: 200, Duration: 100 * time.Millisecond},
				{StatusCode: 200, Duration: 300 * time.Millisecond},
			},
			wantSuccessful:  2,
			wantFailed:      0,
			wantAvg:         200 * time.Millisecond,
			wantStatusCodes: map[int]int{200: 2},
		},
		{
			name: "all failed with errors",
			results: []Result{
				{Error: errors.New("connection refused")},
				{Error: errors.New("timeout")},
			},
			wantSuccessful:  0,
			wantFailed:      2,
			wantAvg:         0,
			wantStatusCodes: map[int]int{},
		},
		{
			name: "mixed success and 5xx",
			results: []Result{
				{StatusCode: 200, Duration: 100 * time.Millisecond},
				{StatusCode: 500, Duration: 100 * time.Millisecond},
			},
			wantSuccessful:  1,
			wantFailed:      1,
			wantAvg:         100 * time.Millisecond,
			wantStatusCodes: map[int]int{200: 1, 500: 1},
		},
		{
			name:            "empty results",
			results:         []Result{},
			wantSuccessful:  0,
			wantFailed:      0,
			wantAvg:         0,
			wantStatusCodes: map[int]int{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results := make(chan Result, len(tc.results))
			for _, r := range tc.results {
				results <- r
			}
			close(results)

			lt := &LoadTester{
				config:  Config{TotalReqs: len(tc.results)},
				results: results,
			}

			lt.collectResults()

			if lt.stats.Successful != tc.wantSuccessful {
				t.Errorf("Successful = %d, want %d", lt.stats.Successful, tc.wantSuccessful)
			}
			if lt.stats.Failed != tc.wantFailed {
				t.Errorf("Failed = %d, want %d", lt.stats.Failed, tc.wantFailed)
			}
			if lt.stats.AvgResponseTime != tc.wantAvg {
				t.Errorf("AvgResponseTime = %v, want %v", lt.stats.AvgResponseTime, tc.wantAvg)
			}
			if len(lt.stats.StatusCodes) != len(tc.wantStatusCodes) {
				t.Errorf("StatusCodes = %v, want %v", lt.stats.StatusCodes, tc.wantStatusCodes)
			}
		})
	}
}

func TestExecuteRequest(t *testing.T) {
	t.Run("successful GET", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		lt := &LoadTester{ctx: context.Background(), client: &http.Client{}}

		result := lt.executeRequest(Job{ID: 1, Method: "GET", URL: srv.URL})

		if result.Error != nil {
			t.Fatalf("unexpected error: %v", result.Error)
		}
		if result.StatusCode != http.StatusOK {
			t.Errorf("StatusCode = %d, want %d", result.StatusCode, http.StatusOK)
		}
		if result.JobID != 1 {
			t.Errorf("JobID = %d, want 1", result.JobID)
		}
	})

	t.Run("POST sets Content-Type", func(t *testing.T) {
		var contentType string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			contentType = r.Header.Get("Content-Type")
			w.WriteHeader(http.StatusCreated)
		}))
		defer srv.Close()

		lt := &LoadTester{ctx: context.Background(), client: &http.Client{}}

		result := lt.executeRequest(Job{ID: 2, Method: "POST", URL: srv.URL, Body: []byte(`{"a":1}`)})

		if result.Error != nil {
			t.Fatalf("unexpected error: %v", result.Error)
		}
		if result.StatusCode != http.StatusCreated {
			t.Errorf("StatusCode = %d, want %d", result.StatusCode, http.StatusCreated)
		}
		if contentType != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", contentType)
		}
	})

	t.Run("server returns 500", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		lt := &LoadTester{ctx: context.Background(), client: &http.Client{}}

		result := lt.executeRequest(Job{ID: 3, Method: "GET", URL: srv.URL})

		if result.Error != nil {
			t.Fatalf("unexpected error: %v", result.Error)
		}
		if result.StatusCode != http.StatusInternalServerError {
			t.Errorf("StatusCode = %d, want %d", result.StatusCode, http.StatusInternalServerError)
		}
	})

	t.Run("malformed URL returns error", func(t *testing.T) {
		lt := &LoadTester{ctx: context.Background(), client: &http.Client{}}

		result := lt.executeRequest(Job{ID: 4, Method: "GET", URL: "://bad-url"})

		if result.Error == nil {
			t.Error("expected error for malformed URL, got nil")
		}
	})

	t.Run("connection refused returns error", func(t *testing.T) {
		lt := &LoadTester{ctx: context.Background(), client: &http.Client{Timeout: time.Second}}

		result := lt.executeRequest(Job{ID: 5, Method: "GET", URL: "http://localhost:0"})

		if result.Error == nil {
			t.Error("expected error, got nil")
		}
	})
}

func TestRun(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   50,
		Concurrency: 5,
		Timeout:     time.Second,
		Method:      "GET",
	}

	lt := NewLoadTester(context.Background(), cfg)
	stats, err := lt.Run()
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if stats.TotalRequests != 50 {
		t.Errorf("TotalRequests = %d, want 50", stats.TotalRequests)
	}
	if stats.Successful != 50 {
		t.Errorf("Successful = %d, want 50", stats.Successful)
	}
	if stats.Failed != 0 {
		t.Errorf("Failed = %d, want 0", stats.Failed)
	}
	if len(stats.StatusCodes) != 1 || stats.StatusCodes[200] != 50 {
		t.Errorf("StatusCodes = %v, want map[200:50]", stats.StatusCodes)
	}
	if stats.RequestsPerSec <= 0 {
		t.Errorf("RequestsPerSec = %v, want > 0", stats.RequestsPerSec)
	}
}

func TestRunCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   1000,
		Concurrency: 10,
		Timeout:     time.Second,
		Method:      "GET",
	}

	lt := NewLoadTester(ctx, cfg)

	done := make(chan struct{})
	go func() {
		lt.Run()
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Run returned after cancellation — no deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation — possible deadlock")
	}
}

func TestRateLimiting(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   100,
		Concurrency: 10,
		Timeout:     time.Second,
		Method:      "GET",
		Rate:        50,
	}

	lt := NewLoadTester(context.Background(), cfg)

	start := time.Now()

	if _, err := lt.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	elapsed := time.Since(start)

	if elapsed < 500*time.Millisecond {
		t.Errorf("rate limiter not working: 100 req at 50/sec took %v, want >= 500ms", elapsed)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"valid", Config{TotalReqs: 10, Concurrency: 2, Rate: 10}, false},
		{"zero rate is unlimited", Config{TotalReqs: 10, Concurrency: 2, Rate: 0}, false},
		{"negative rate invalid", Config{TotalReqs: 10, Concurrency: 2, Rate: -1}, true},
		{"zero total invalid", Config{TotalReqs: 0, Concurrency: 2}, true},
		{"zero concurrency invalid", Config{TotalReqs: 10, Concurrency: 0}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestJSONRepost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   10,
		Concurrency: 2,
		Timeout:     time.Second,
		Method:      "GET",
	}

	lt := NewLoadTester(context.Background(), cfg)
	if _, err := lt.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	data, err := lt.JSONReport()
	if err != nil {
		t.Fatalf("JSONReport returned error: %v", err)
	}

	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if report.TotalRequests != 10 {
		t.Errorf("TotalRequests = %d, want 10", report.TotalRequests)
	}
	if report.Successful != 10 {
		t.Errorf("Successful = %d, want 10", report.Successful)
	}
	if report.URL != srv.URL {
		t.Errorf("URL = %q, want %q", report.URL, srv.URL)
	}
	if report.StatusCodes[200] != 10 {
		t.Errorf("StatusCodes[200] = %d, want 10", report.StatusCodes[200])
	}
}
