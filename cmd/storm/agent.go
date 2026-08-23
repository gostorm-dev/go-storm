package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/gostorm-dev/go-storm/internal/dist"
	"github.com/gostorm-dev/go-storm/internal/metrics"
	"github.com/gostorm-dev/go-storm/pkg/storm"
	"github.com/spf13/cobra"
)

var (
	agentName        string
	agentConcurrency int
	agentTimeout     int
	agentMetricsPort int
	agentStayAlive   bool
)

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Run a distributed agent that pulls jobs from Redis",
	Long: `agent connects to Redis, registers itself, pulls jobs from the shared
queue, executes them, and pushes the results back.

Run multiple agents (on the same machine or different ones) and drive them
all with a single 'storm run-dist' command.`,
	Example: `  # Start agent with default settings
  storm agent

  # Start named agent with 20 workers
  storm agent --name agent-1 -c 20

  # Start agent with custom Redis
  storm agent --redis 10.0.1.5:6379

  # Start agent with metrics
  storm agent --metrics-port 9091 --stay-alive`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		id := agentID(agentName)

		rdb := dist.NewRedis(redisAddr)
		if err := rdb.Ping(ctx); err != nil {
			return fmt.Errorf("cannot connect to redis at %s: %w", redisAddr, err)
		}

		if agentMetricsPort > 0 {
			metricsServer := &http.Server{
				Addr:    fmt.Sprintf(":%d", agentMetricsPort),
				Handler: metrics.Handler(),
			}
			go func() {
				fmt.Printf("Metrics: http://localhost:%d/metrics\n", agentMetricsPort)
				if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					fmt.Fprintf(os.Stderr, "metrics server: %v\n", err)
				}
			}()
			defer metricsServer.Shutdown(context.Background())
		}

		color.Green("Agent %s started (connected to %s)", id, redisAddr)
		fmt.Printf("Workers: %d\n", agentConcurrency)
		fmt.Printf("Idle exit after 5s with no jobs (Ctrl+C to stop)\n")

		processed, err := rdb.RunAgent(ctx, id, agentConcurrency,
			time.Duration(agentTimeout)*time.Second,
			agentStayAlive,
			func(storm.Job) { metrics.RequestStart() },
			metrics.Record,
		)
		if err != nil {
			return err
		}
		fmt.Printf("\nAgent %s processed %d requests\n", id, processed)
		return nil
	},
}

// agentID returns the given name or generates one from hostname + timestamp.
func agentID(name string) string {
	if name != "" {
		return name
	}
	host, err := os.Hostname()
	if err != nil {
		host = "agent"
	}
	return fmt.Sprintf("%s-%d", host, time.Now().UnixNano())
}

func init() {
	agentCmd.Flags().StringVar(&redisAddr, "redis", "localhost:6379", "Redis address of the coordinator")
	agentCmd.Flags().StringVar(&agentName, "name", "", "Agent name (default: hostname-timestamp)")
	agentCmd.Flags().IntVarP(&agentConcurrency, "concurrency", "c", 5, "Agent worker goroutines")
	agentCmd.Flags().IntVarP(&agentTimeout, "timeout", "t", 10, "Request timeout in seconds")
	agentCmd.Flags().IntVar(&agentMetricsPort, "metrics-port", 9091, "Prometheus /metrics port (0 = disabled)")
	agentCmd.Flags().BoolVar(&agentStayAlive, "stay-alive", false, "Keep running after the queue empties (for metrics)")
}
