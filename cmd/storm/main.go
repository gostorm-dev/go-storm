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
	flags := config.ParseFlags()
	cfg := flags.Config

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

	switch flags.Format {
	case "json":
		data, err := tester.JSONReport()
		if err != nil {
			log.Fatal("JSON report failed:", err)
		}
		if flags.Output != "" {
			if err := os.WriteFile(flags.Output, data, 0644); err != nil {
				log.Fatal("write report:", err)
			}
			fmt.Printf("Report saved to %s\n", flags.Output)
		}
		fmt.Println(string(data))
	default:
		tester.PrintStats()
	}
}
