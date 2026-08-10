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
)

var (
	agentName        string
	agentConcurrency int
	agentTimeout     int
)

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Run a distributed agent that pulls jobs from Redis",
	Long: `agent connects to Redis, registers itself, pulls jobs from the shared
queue, executes them, and pushes the results back.

Run multiple agents (on the same machine or different ones) and drive them
all with a single 'storm run-dist' command.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		id := agentID(agentName)

		rdb := dist.NewRedis(redisAddr)
		if err := rdb.Ping(ctx); err != nil {
			return fmt.Errorf("cannot connect to redis at %s: %w", redisAddr, err)
		}

		color.Green("Agent %s started (connected to %s)", id, redisAddr)
		fmt.Printf("Workers: %d\n", agentConcurrency)
		fmt.Printf("Idle exit after 5s with no jobs (Ctrl+C to stop)\n")

		processed, err := rdb.RunAgent(ctx, id, agentConcurrency, time.Duration(agentTimeout)*time.Second)
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
	agentCmd.Flags().StringVar(&agentName, "name", "", "Agent name (default: hostname-timestamp)")
	agentCmd.Flags().IntVarP(&agentConcurrency, "concurrency", "c", 5, "Agent worker goroutines")
	agentCmd.Flags().IntVarP(&agentTimeout, "timeout", "t", 10, "Request timeout in seconds")
}
