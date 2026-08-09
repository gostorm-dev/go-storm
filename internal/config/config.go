// Package config handles CLI flag parsing.
// It lives in internal/ because it's CLI-specific — external library
// users of pkg/storm don't need flag parsing, they build Config directly.
package config

import (
	"flag"
	"time"

	"github.com/hariomop12/go-storm/pkg/storm"
)

// ParseFlags reads the CLI flags and returns a storm.Config.
// Returning storm.Config (not a config-specific type) keeps a single
// source of truth for what a load test needs.

type Flags struct {
	Config storm.Config
	Format string
	Output string
}

func ParseFlags() Flags {
	url := flag.String("url", "https://hariomtanu.com", "Target URL")
	total := flag.Int("n", 100, "Total requests")
	concurrency := flag.Int("c", 10, "Concurrency level")
	method := flag.String("method", "GET", "HTTP Method")
	timeout := flag.Int("timeout", 10, "Request timeout in seconds")
	rate := flag.Int("rate", 0, "Max requests per second (0 = unlimited)")
	format := flag.String("format", "text", "Output format: text or json")
	output := flag.String("output", "", "Write report to a file")
	body := flag.String("body", "", "Request body, e.g. --body '{\"name\":\"hariom\"}'")

	flag.Parse()

	var payload []byte
	switch {
	case *body != "":
		payload = []byte(*body)
	case *method == "POST" || *method == "PUT":
		payload = []byte(`{"test":"data"}`)
	}

	return Flags{
		Config: storm.Config{
			URL:         *url,
			TotalReqs:   *total,
			Concurrency: *concurrency,
			Timeout:     time.Duration(*timeout) * time.Second,
			Method:      *method,
			Payload:     payload,
			Rate:        *rate,
		},
		Format: *format,
		Output: *output,
	}
}
