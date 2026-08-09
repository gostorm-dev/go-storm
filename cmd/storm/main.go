// Command storm is the CLI entry point for the load tester.
// It stays thin: parse flags -> build tester -> run -> print stats.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/hariomop12/go-storm/internal/config"
	"github.com/hariomop12/go-storm/pkg/storm"
)

func main() {
	cfg := config.ParseFlags()

	fmt.Println("Starting Load Test")
	fmt.Printf("Target: %s\n", cfg.URL)
	fmt.Printf("Total: %d requests\n", cfg.TotalReqs)
	fmt.Printf("Concurrency: %d workers\n", cfg.Concurrency)
	fmt.Printf("Rate: %d req/sec\n", cfg.Rate)
	fmt.Println("Running...")

	// signal.NotifyContext wires Ctrl+C / SIGTERM into the context,
	// so workers stop cleanly instead of the process being killed.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tester := storm.NewLoadTester(ctx, cfg)
	if _, err := tester.Run(); err != nil {
		log.Fatal("Test failed:", err)
	}
	tester.PrintStats()
}
