package main

import (
	"strings"
	"testing"
)

func TestDisplayWidth_IgnoresANSI(t *testing.T) {
	plain := "go-storm dev"
	colored := "\x1b[1m" + plain + "\x1b[0m"
	if displayWidth(colored) != displayWidth(plain) {
		t.Fatalf("ANSI escapes counted as width: plain=%d colored=%d",
			displayWidth(plain), displayWidth(colored))
	}
}

func TestDisplayWidth_WideLightning(t *testing.T) {
	if got := displayWidth("⚡"); got != 2 {
		t.Fatalf("⚡ must measure 2 cells, got %d", got)
	}
	if got := displayWidth("a"); got != 1 {
		t.Fatalf("ASCII must measure 1 cell, got %d", got)
	}
}

// TestBuildBanner_AllLinesEqualWidth is the guarantee: for ANY version
// string — short, long, dirty — every banner line occupies exactly the
// same number of terminal cells, so the walls always line up.
func TestBuildBanner_AllLinesEqualWidth(t *testing.T) {
	versions := []string{
		"dev",
		"v0.5.1",
		"v0.5.1-1-gb141a1e-dirty",
		"v0.6.0-42-g1a2b3c4d",
		strings.Repeat("v9.9.9-alpha.", 8), // absurdly long — still must align
	}

	for _, v := range versions {
		t.Run(v, func(t *testing.T) {
			version = v
			defer func() { version = "dev" }()

			lines := buildBanner()
			var widths []int
			for _, l := range lines {
				if strings.TrimSpace(l) == "" {
					continue
				}
				widths = append(widths, displayWidth(l))
			}
			for i := 1; i < len(widths); i++ {
				if widths[i] != widths[0] {
					t.Fatalf("misaligned banner for version %q:\nwidths=%v\n%s",
						v, widths, strings.Join(lines, "\n"))
				}
			}
		})
	}
}

func TestBuildBanner_ContainsContent(t *testing.T) {
	version = "v9.9.9-test"
	defer func() { version = "dev" }()

	out := strings.Join(buildBanner(), "\n")
	for _, want := range []string{"go-storm v9.9.9-test", "The Load Tester That Tells Truth"} {
		if !strings.Contains(out, want) {
			t.Fatalf("banner missing %q:\n%s", want, out)
		}
	}
}
