package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"github.com/hariomop12/go-storm/internal/config"
	"github.com/hariomop12/go-storm/pkg/storm"
)

var (
	url         string
	total       int
	concurrency int
	method      string
	timeout     int
	rate        int
	body        string
	format      string
	output      string
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a load test against a URL",
	Long: `Run sends N requests to a URL using a pool of concurrent workers.
Optionally throttle throughput with --rate, and export results as JSON.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		opts := config.Build(url, method, body, total, concurrency, timeout, rate)
		opts.Format = format
		opts.Output = output
		cfg := opts.Config

		color.Cyan("Starting Load Test")
		fmt.Printf("Target: %s\n", cfg.URL)
		fmt.Printf("Total: %d requests\n", cfg.TotalReqs)
		fmt.Printf("Concurrency: %d workers\n", cfg.Concurrency)
		fmt.Printf("Rate: %d req/sec\n", cfg.Rate)

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		tester := storm.NewLoadTester(ctx, cfg)

		// progress bar
		var bar *progressbar.ProgressBar
		var done chan struct{}
		if opts.Format == "text" {
			bar = progressbar.NewOptions64(int64(cfg.TotalReqs),
				progressbar.OptionSetDescription("Running"),
				progressbar.OptionSetWidth(40),
				progressbar.OptionShowCount(),
				progressbar.OptionSetRenderBlankState(true),
			)

			// The board-watcher goroutine: peeks at the counter every 500ms
			done = make(chan struct{})
			go func() {
				ticker := time.NewTicker(500 * time.Millisecond)
				defer ticker.Stop()

				prevCount := int64(0)
				prevTime := time.Now()

				for {
					select {
					case <-done:
						return
					case now := <-ticker.C:
						count := tester.Completed()
						rps := float64(count-prevCount) / now.Sub(prevTime).Seconds()
						prevCount, prevTime = count, now

						bar.Describe(color.GreenString("%.0f req/s", rps))
						bar.Set64(count)
					}
				}
			}()
		}

		if _, err := tester.Run(); err != nil {
			if bar != nil {
				close(done)
				bar.Finish()
			}
			return err
		}

		// Stop the watcher and finalize the bar
		if bar != nil {
			close(done)
			bar.Set64(tester.Completed())
			bar.Finish()
			fmt.Println()
		}

		switch opts.Format {
		case "json":
			data, err := tester.JSONReport()
			if err != nil {
				return err
			}
			if opts.Output != "" {
				if err := os.WriteFile(opts.Output, data, 0644); err != nil {
					return err
				}
				color.Green("Report saved to %s", opts.Output)
			}
			fmt.Println(string(data))
		default:
			tester.PrintStats()
		}
		return nil
	},
}

func init() {
	runCmd.Flags().StringVarP(&url, "url", "u", "https://hariomtanu.com", "Target URL")
	runCmd.Flags().IntVarP(&total, "requests", "n", 100, "Total requests to send")
	runCmd.Flags().IntVarP(&concurrency, "concurrency", "c", 10, "Concurrency level (parallel workers)")
	runCmd.Flags().StringVarP(&method, "method", "m", "GET", "HTTP method: GET, POST, PUT, DELETE")
	runCmd.Flags().IntVarP(&timeout, "timeout", "t", 10, "Request timeout in seconds")
	runCmd.Flags().IntVarP(&rate, "rate", "r", 0, "Max requests per second (0 = unlimited)")
	runCmd.Flags().StringVarP(&body, "body", "b", "", "Request body (for POST/PUT)")
	runCmd.Flags().StringVar(&format, "format", "text", "Output format: text or json")
	runCmd.Flags().StringVar(&output, "output", "", "Write JSON report to a file")
}
