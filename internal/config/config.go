// Package config handles CLI flag parsing.
// It lives in internal/ because it's CLI-specific — external library
// users of pkg/storm don't need flag parsing, they build Config directly.
package config

import (
	"time"

	"github.com/hariomop12/go-storm/pkg/storm"
)

// ParseFlags reads the CLI flags and returns a storm.Config.
// Returning storm.Config (not a config-specific type) keeps a single
// source of truth for what a load test needs.

type Options struct {
	Config storm.Config
	Format string
	Output string
}

func Build(url, method, body string, total, concurrency, timeout, rate int) Options {
	var payload []byte
	switch {
	case body != "":
		payload = []byte(body)
	case method == "POST" || method == "PUT":
		payload = []byte(`{"test": "data"}`)
	}

	return Options{
		Config: storm.Config{
			URL:         url,
			TotalReqs:   total,
			Concurrency: concurrency,
			Timeout:     time.Duration(timeout) * time.Second,
			Method:      method,
			Payload:     payload,
			Rate:        rate,
		},
	}
}
