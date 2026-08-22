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

	"github.com/gostorm-dev/go-storm/internal/config"
	"github.com/gostorm-dev/go-storm/internal/metrics"
	"github.com/gostorm-dev/go-storm/internal/transport"
	"github.com/gostorm-dev/go-storm/pkg/storm"
)

var (
	url            string
	total          int
	duration       time.Duration
	concurrency    int
	method         string
	timeout        int
	rate           int
	body           string
	headers        []string
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
	Long: `Run sends N requests — or runs for a fixed duration — against a URL
using a pool of concurrent workers. Optionally throttle throughput with
--rate, and export results as JSON.`,
	Example: `  # Basic load test
  storm run -u https://example.com -n 1000 -c 50

  # Sustained load for 5 minutes
  storm run -u https://example.com -d 5m -c 200

  # Constant arrival rate: 1000 RPS for 30 minutes
  storm run -u https://example.com -d 30m -c 100 -r 1000

  # Rate limited test (1000 RPS)
  storm run -u https://example.com -n 5000 -c 100 -r 1000

  # POST with body
  storm run -u https://api.example.com/users -m POST -b '{"name":"test"}'

  # Authenticated request with custom headers
  storm run -u https://api.example.com/me -H "Authorization: Bearer $TOKEN" -H "X-Trace: loadtest"

  # Capacity estimation
  storm run -u https://example.com -n 5000 --estimate

  # Save JSON report
  storm run -u https://example.com -n 1000 --format json --output result.json

  # Prometheus metrics
  storm run -u https://example.com -n 5000 --metrics-port 9091

  # High-performance with connection pooling
  storm run -u https://example.com -n 100000 --max-idle-conns 500 --max-idle-per-host 100`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if url == "" {
			return fmt.Errorf("no URL specified — use: storm run -u https://example.com")
		}
		validMethods := map[string]bool{"GET": true, "POST": true, "PUT": true, "DELETE": true, "PATCH": true, "HEAD": true}
		if !validMethods[method] {
			return fmt.Errorf("invalid HTTP method '%s' — valid: GET, POST, PUT, DELETE, PATCH, HEAD", method)
		}
		switch {
		case total > 0 && duration > 0:
			return fmt.Errorf(
				"--requests (-n) and --duration (-d) are mutually exclusive: "+
					"got n=%d AND d=%s — pick one workload definition",
				total, duration)
		case total <= 0 && duration <= 0:
			return fmt.Errorf("no workload defined: set --requests (-n) OR --duration (-d)")
		}
		if concurrency <= 0 {
			return fmt.Errorf("concurrency must be greater than 0 — got %d", concurrency)
		}
		if format != "text" && format != "json" && format != "table" && format != "quiet" && format != "csv" {
			return fmt.Errorf("invalid format '%s' — valid: text, json, table, quiet, csv", format)
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		opts := config.Build(url, method, body, total, concurrency, timeout, rate, duration)
		opts.Format = format
		opts.Output = output

		// Brand banner for human-readable output only — json/csv/quiet stay clean.
		if opts.Format == "" || opts.Format == "text" || opts.Format == "table" {
			printBanner()
		}

		cfg := opts.Config

		cfg.Headers = http.Header{}
		for _, h := range headers {
			k, v, err := storm.ParseHeaderSpec(h)
			if err != nil {
				return err
			}
			cfg.Headers.Add(k, v)
		}

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
			color.New(color.FgCyan).Println("Running capacity estimation (3 seconds)...")
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
				color.New(color.FgYellow).Printf("Capacity estimation failed: %v\n", err)
			} else {
				targetRPS := float64(cfg.Rate)
				fmt.Println(storm.FormatCapacityReport(cr, targetRPS))
			}
		}

		// --- Print config ---
		if opts.Format == "text" || opts.Format == "" || opts.Format == "table" {
			bold := color.New(color.FgWhite, color.Bold).SprintFunc()
			dim := color.New(color.FgHiBlack).SprintFunc()

			fmt.Printf("%s %s\n", bold("Target:"), cfg.URL)
			if cfg.Duration > 0 {
				fmt.Printf("%s %s\n", bold("Duration:"), cfg.Duration)
			} else {
				fmt.Printf("%s %s\n", bold("Requests:"), fmt.Sprintf("%d", cfg.TotalReqs))
			}
			fmt.Printf("%s %s\n", bold("Workers:"), fmt.Sprintf("%d", cfg.Concurrency))
			if cfg.Rate > 0 {
				fmt.Printf("%s %s\n", bold("Rate:"), fmt.Sprintf("%d req/sec", cfg.Rate))
			}
			if saturation {
				mode := dim("warn only")
				if saturationKill {
					mode = color.New(color.FgRed).Sprint("kill on critical")
				}
				fmt.Printf("%s %s\n", bold("Saturation:"), mode)
			}
			fmt.Println()
		}

		tester := storm.NewLoadTester(ctx, cfg)

		// Enable saturation monitoring
		if saturation {
			tester.EnableSaturationMonitoring()
			if saturationKill {
				tester.EnableSaturationKill()
			}
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
				color.New(color.FgCyan).Printf("Metrics: http://localhost:%d/metrics\n", metricsPort)
				if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					fmt.Fprintf(os.Stderr, "metrics server: %v\n", err)
				}
			}()
			defer metricsServer.Shutdown(context.Background())
		}

		// progress bar
		var bar *progressbar.ProgressBar
		var done chan struct{}
		if opts.Format == "text" || opts.Format == "" {
			// Duration mode tracks elapsed time; count mode tracks completed
			// requests. The ms-granularity max keeps the percentage smooth.
			timeMode := cfg.Duration > 0
			barOpts := []progressbar.Option{
				progressbar.OptionSetDescription("Running"),
				progressbar.OptionSetWidth(40),
				progressbar.OptionSetRenderBlankState(true),
			}
			if !timeMode {
				barOpts = append(barOpts, progressbar.OptionShowCount())
			}

			var barMax int64
			if timeMode {
				barMax = int64(cfg.Duration / time.Millisecond)
			} else {
				barMax = int64(cfg.TotalReqs)
			}
			bar = progressbar.NewOptions64(barMax, barOpts...)

			// The board-watcher goroutine: peeks at the counter every 500ms
			done = make(chan struct{})
			start := time.Now()
			go func() {
				ticker := time.NewTicker(500 * time.Millisecond)
				defer ticker.Stop()

				prevCount := int64(0)
				prevTime := start

				for {
					select {
					case <-done:
						return
					case now := <-ticker.C:
						count := tester.Completed()
						rps := float64(count-prevCount) / now.Sub(prevTime).Seconds()
						prevCount, prevTime = count, now

						if timeMode {
							elapsed := now.Sub(start)
							if elapsed > cfg.Duration {
								// Cap at 100% during the graceful drain tail.
								elapsed = cfg.Duration
							}
							bar.Set64(int64(elapsed / time.Millisecond))
						} else {
							bar.Set64(count)
						}
						bar.Describe(color.GreenString("%.0f req/s", rps))
					}
				}
			}()
		}

		finalStats, err := tester.Run()
		if err != nil {
			if bar != nil {
				close(done)
				bar.Finish()
			}
			return err
		}

		// Stop the watcher and finalize the bar
		if bar != nil {
			close(done)
			if cfg.Duration > 0 {
				bar.Set64(int64(cfg.Duration / time.Millisecond))
			} else {
				bar.Set64(tester.Completed())
			}
			bar.Finish()
			fmt.Println()
		}

		// --- Generate the JSON report when needed ---
		var jsonOut []byte
		if opts.Format == "json" || opts.Output != "" {
			data, err := tester.JSONReport()
			if err != nil {
				return err
			}
			jsonOut = data
		}

		// --- Save the report to a file if requested ---
		if opts.Output != "" {
			if err := os.WriteFile(opts.Output, jsonOut, 0644); err != nil {
				return fmt.Errorf("failed to write report to %s: %w", opts.Output, err)
			}
			color.New(color.FgGreen).Printf("Report saved to %s\n", opts.Output)
		}

		// --- Print results ---
		switch opts.Format {
		case "json":
			fmt.Println(string(jsonOut))
		case "table":
			tester.PrintStatsTable()
		case "quiet":
			tester.PrintStatsQuiet()
		case "csv":
			tester.PrintStatsCSV()
		default:
			tester.PrintStats()
		}

		// --- Step 4: Health report ---
		if saturation && (opts.Format == "text" || opts.Format == "" || opts.Format == "table") {
			hr := tester.GetHealthReport()
			if hr != nil {
				fmt.Println(storm.FormatHealthReport(*hr))
			}
		}

		// --- CI gate: opt-in threshold evaluation (exit 2 on violation) ---
		if violation := evaluateThresholds(finalStats); violation != "" {
			fmt.Fprintf(os.Stderr, "\nFAIL: %s\n", violation)
			os.Exit(2)
		}

		return nil
	},
}

func init() {
	runCmd.Flags().StringVarP(&url, "url", "u", "", "Target URL (required)")
	runCmd.Flags().IntVarP(&total, "requests", "n", 0, "Total requests to send (mutually exclusive with --duration)")
	runCmd.Flags().DurationVarP(&duration, "duration", "d", 0, "Test duration: 30s, 5m, 1h (mutually exclusive with --requests)")
	runCmd.Flags().IntVarP(&concurrency, "concurrency", "c", 10, "Concurrency level (parallel workers)")
	runCmd.Flags().StringVarP(&method, "method", "m", "GET", "HTTP method: GET, POST, PUT, DELETE")
	runCmd.Flags().IntVarP(&timeout, "timeout", "t", 10, "Request timeout in seconds")
	runCmd.Flags().IntVarP(&rate, "rate", "r", 0, "Max requests per second (0 = unlimited)")
	runCmd.Flags().StringVarP(&body, "body", "b", "", "Request body (for POST/PUT)")
	runCmd.Flags().StringArrayVarP(&headers, "header", "H", nil, "Custom header (repeatable): -H \"Key: Value\"")
	runCmd.Flags().IntVar(&failAboveErrors, "fail-above-errors", -1, "Exit with code 2 if failed requests exceed N (-1 = disabled)")
	runCmd.Flags().Float64Var(&failAboveP95, "fail-above-p95", -1, "Exit with code 2 if p95 latency exceeds MS milliseconds (-1 = disabled)")
	runCmd.Flags().StringVar(&format, "format", "text", "Output format: text, json, table, quiet, csv")
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
