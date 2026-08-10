package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/hariomop12/go-storm/internal/dist"
	"github.com/hariomop12/go-storm/pkg/storm"
)

var runDistCmd = &cobra.Command{
	Use:   "run-dist",
	Short: "Run a distributed load test using Redis agents",
	Long: `run-dist pushes N jobs into the Redis queue and waits for the agents
to process them all. Any number of agents can be running; each pulls jobs
from the shared queue and pushes results back.

Start agents with 'storm agent' on the machines you want to load from,
then run this command once.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if total <= 0 {
			return fmt.Errorf("total requests must be positive, got %d", total)
		}

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		rdb := dist.NewRedis(redisAddr)
		if err := rdb.Ping(ctx); err != nil {
			return fmt.Errorf("cannot connect to redis at %s: %w", redisAddr, err)
		}
		if err := rdb.Flush(ctx); err != nil {
			return err
		}

		cfg := storm.Config{
			URL:     url,
			Method:  method,
			Timeout: time.Duration(timeout) * time.Second,
			Payload: []byte(body),
		}

		color.Cyan("Distributed Load Test")
		fmt.Printf("Target: %s\n", cfg.URL)
		fmt.Printf("Total: %d requests\n", total)
		fmt.Printf("Queue: %s\n\n", redisAddr)

		jobs := make([]storm.Job, total)
		for i := range jobs {
			jobs[i] = storm.Job{
				ID:     i + 1,
				URL:    cfg.URL,
				Method: cfg.Method,
				Body:   cfg.Payload,
			}
		}

		// Live result counter (text output only).
		var done chan struct{}
		if format == "text" {
			done = make(chan struct{})
			go func() {
				ticker := time.NewTicker(500 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-ticker.C:
						n, err := rdb.ResultsCount(ctx)
						if err == nil {
							fmt.Printf("\rResults: %d/%d", n, total)
						}
					}
				}
			}()
		}

		stats, err := rdb.RunCoordinator(ctx, jobs)
		if done != nil {
			close(done)
			fmt.Println()
		}
		if err != nil {
			return err
		}

		// Final output in the same formats as local runs.
		if format == "json" {
			data, err := storm.ReportJSON(cfg, stats)
			if err != nil {
				return err
			}
			if output != "" {
				if err := os.WriteFile(output, data, 0644); err != nil {
					return err
				}
				color.Green("Report saved to %s", output)
			}
			fmt.Println(string(data))
			return nil
		}
		storm.PrintStatsReport(cfg, stats)
		return nil
	},
}

func init() {
	runDistCmd.Flags().StringVarP(&url, "url", "u", "https://hariomtanu.com", "Target URL")
	runDistCmd.Flags().IntVarP(&total, "requests", "n", 100, "Total requests to send")
	runDistCmd.Flags().StringVarP(&method, "method", "m", "GET", "HTTP method: GET, POST, PUT, DELETE")
	runDistCmd.Flags().IntVarP(&timeout, "timeout", "t", 10, "Request timeout in seconds")
	runDistCmd.Flags().StringVarP(&body, "body", "b", "", "Request body (for POST/PUT)")
	runDistCmd.Flags().StringVar(&format, "format", "text", "Output format: text or json")
	runDistCmd.Flags().StringVar(&output, "output", "", "Write JSON report to a file")
}
