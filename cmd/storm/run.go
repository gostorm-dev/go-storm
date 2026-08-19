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
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"github.com/hariomop12/go-storm/internal/config"
	"github.com/hariomop12/go-storm/internal/metrics"
	"github.com/hariomop12/go-storm/internal/transport"
	"github.com/hariomop12/go-storm/pkg/storm"
)

var (
	url            string
	total          int
	concurrency    int
	method         string
	timeout        int
	rate           int
	body           string
	format         string
	output         string
	metricsPort    int
	saturation     bool
	estimate       bool
	saturationKill bool

	// Connection pooling flags
	maxIdleConns   int
	maxIdlePerHost int
	idleTimeout    int
	keepAlive      int
	forceHTTP2     bool
	insecure       bool
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

		// Create transport config from CLI flags
		transportCfg := transport.DefaultConfig()
		transportCfg.MaxIdleConns = maxIdleConns
		transportCfg.MaxIdleConnsPerHost = maxIdlePerHost
		transportCfg.IdleConnTimeout = time.Duration(idleTimeout) * time.Second
		transportCfg.KeepAlive = time.Duration(keepAlive) * time.Second
		transportCfg.ForceHTTP2 = forceHTTP2
		transportCfg.InsecureSkipVerify = insecure
		cfg.TransportConfig = &transportCfg

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		// --- Step 1: Capacity estimation ---
		if estimate {
			color.Cyan("Running capacity estimation (3 seconds)...")
			fmt.Println()

			capCtx, capCancel := context.WithTimeout(ctx, 10*time.Second)
			defer capCancel()

			cr, err := storm.EstimateCapacity(
				capCtx,
				cfg.URL,
				cfg.Method,
				cfg.Concurrency,
				200,
				cfg.Timeout,
			)
			if err != nil {
				color.Yellow("Capacity estimation failed: %v", err)
			} else {
				targetRPS := float64(cfg.Rate)
				fmt.Println(storm.FormatCapacityReport(cr, targetRPS))
			}
		}

		// --- Print config ---
		color.Cyan("Starting Load Test")
		fmt.Printf("Target: %s\n", cfg.URL)
		fmt.Printf("Total: %d requests\n", cfg.TotalReqs)
		fmt.Printf("Concurrency: %d workers\n", cfg.Concurrency)
		fmt.Printf("Rate: %d req/sec\n", cfg.Rate)
		if saturation {
			fmt.Printf("Saturation: enabled")
			if saturationKill {
				fmt.Printf(" (kill on critical)")
			} else {
				fmt.Printf(" (warn only)")
			}
			fmt.Println()
		}

		tester := storm.NewLoadTester(ctx, cfg)

		// Enable saturation monitoring
		if saturation {
			tester.EnableSaturationMonitoring()
		}

		// Optional Prometheus metrics endpoint
		if metricsPort > 0 {
			tester.SetHooks(
				func(storm.Job) { metrics.RequestStart() },
				metrics.Record,
			)
			metricsServer := &http.Server{
				Addr:    fmt.Sprintf(":%d", metricsPort),
				Handler: metrics.Handler(),
			}
			go func() {
				fmt.Printf("Metrics: http://localhost:%d/metrics\n", metricsPort)
				if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					fmt.Fprintf(os.Stderr, "metrics server: %v\n", err)
				}
			}()
			defer metricsServer.Shutdown(context.Background())
		}

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

		// --- Print results ---
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

		// --- Step 4: Health report ---
		if saturation {
			hr := tester.GetHealthReport()
			if hr != nil {
				fmt.Println(storm.FormatHealthReport(*hr))
			}
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
	runCmd.Flags().IntVar(&metricsPort, "metrics-port", 0, "Prometheus /metrics port (0 = disabled)")
	runCmd.Flags().BoolVar(&saturation, "saturation", true, "Enable generator saturation monitoring")
	runCmd.Flags().BoolVar(&estimate, "estimate", false, "Run capacity estimation before test")
	runCmd.Flags().BoolVar(&saturationKill, "saturation-kill", false, "Kill test on critical saturation (vs warn only)")

	// Connection pooling flags
	runCmd.Flags().IntVar(&maxIdleConns, "max-idle-conns", 200, "Max idle connections across all hosts")
	runCmd.Flags().IntVar(&maxIdlePerHost, "max-idle-per-host", 50, "Max idle connections per target host")
	runCmd.Flags().IntVar(&idleTimeout, "idle-timeout", 90, "Idle connection timeout in seconds")
	runCmd.Flags().IntVar(&keepAlive, "keep-alive", 30, "TCP keep-alive interval in seconds")
	runCmd.Flags().BoolVar(&forceHTTP2, "force-http2", true, "Force HTTP/2 protocol")
	runCmd.Flags().BoolVar(&insecure, "insecure", false, "Skip TLS certificate verification")
}
