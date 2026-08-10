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
	agentConcurrency int
	agentTimeout     int
)

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Run a distributed agent that pulls jobs from Redis",
	Long: `agent connects to Redis, pulls jobs from the shared queue, executes
them, and pushes the results back.

Run multiple agents (on the same machine or different ones) and drive them
all with a single 'storm run-dist' command.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		rdb := dist.NewRedis(redisAddr)
		if err := rdb.Ping(ctx); err != nil {
			return fmt.Errorf("cannot connect to redis at %s: %w", redisAddr, err)
		}

		color.Green("Agent started (connected to %s)", redisAddr)
		fmt.Printf("Workers: %d\n", agentConcurrency)
		fmt.Printf("Idle exit after 5s with no jobs (Ctrl+C to stop)\n")

		processed, err := rdb.RunAgent(ctx, agentConcurrency, time.Duration(agentTimeout)*time.Second)
		if err != nil {
			return err
		}
		fmt.Printf("\nAgent processed %d requests\n", processed)
		return nil
	},
}

func init() {
	agentCmd.Flags().IntVarP(&agentConcurrency, "concurrency", "c", 5, "Agent worker goroutines")
	agentCmd.Flags().IntVarP(&agentTimeout, "timeout", "t", 10, "Request timeout in seconds")
}
