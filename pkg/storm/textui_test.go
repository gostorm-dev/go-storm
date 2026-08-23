package storm

import (
	"math/rand"
	"strings"
	"testing"
)

func TestDisplayWidthStripsANSI(t *testing.T) {
	if got := displayWidth("\x1b[31mabc\x1b[0m"); got != 3 {
		t.Errorf("displayWidth(colored abc) = %d, want 3", got)
	}
}

func TestFitCellPadsAndFitsExactly(t *testing.T) {
	cases := []struct {
		in    string
		width int
	}{
		{"short", 10},
		{"exactly18ch-ok!!", 18},
		{"v0.5.6-42-g1a2b3c4d-dirty-long-version", 18}, // real-world overflow
		{"https://api.example.com/🎉checkout/page", 18},
		{"👨‍👩‍👧‍👦family", 8}, // ZWJ sequence = one grapheme
		{"", 5},
	}
	for _, tc := range cases {
		got := fitCell(tc.in, tc.width)
		if w := displayWidth(got); w != tc.width {
			t.Errorf("fitCell(%q, %d) renders %d columns, want %d: %q",
				tc.in, tc.width, w, tc.width, got)
		}
		if strings.ContainsRune(got, 0xFFFD) {
			t.Errorf("fitCell(%q) produced replacement char — mid-rune cut: %q", tc.in, got)
		}
	}
}

func TestFitCellMarksTruncation(t *testing.T) {
	got := fitCell("2562047h47m16.854775807s", 18)
	if !strings.Contains(got, "..") {
		t.Errorf("expected .. marker before padding: %q", got)
	}
	if !strings.Contains(got, "..") {
		t.Errorf("truncated cell missing .. marker: %q", got)
	}
	if displayWidth(got) != 18 {
		t.Errorf("width = %d, want 18", displayWidth(got))
	}
}

func TestFitCellDegenerateWidths(t *testing.T) {
	for w := 0; w <= 3; w++ {
		got := fitCell("anything-at-all", w)
		if dw := displayWidth(got); dw != w {
			t.Errorf("width=%d: fitCell renders %d columns (%q), want %d", w, dw, got, w)
		}
	}
}

// TestTableRowWidthsIdentical simulates what PrintStatsTable emits: every
// cell passes through padRight/padLeft, so every rendered row must occupy
// the same number of display columns — borders cannot break.
func TestTableRowWidthsIdentical(t *testing.T) {
	rows := [][2]string{
		{"URL", padLeft(truncate("https://api.example.com/🎉checkout/long/path", 18), 18)},
		{"Version", padLeft("v0.5.6-42-g1a2b3c4d-dirty", 18)},
		{"Avg Latency", padLeft("1234567890123456789012345", 18)},
		{"Requested", padLeft("2562047h47m16.854775807s", 18)},
		{"Success Rate", padLeft("100.00%", 18)},
		{"p99.9 Latency", padLeft("999999999.99 ms", 18)},
	}
	// Reference row from known-fitting ASCII cells fixes the exact column
	// count (box-drawing chars are multi-byte but single-width — counting
	// them with len() would repeat the very bug being tested).
	want := displayWidth("  │ " + strings.Repeat("x", 17) + " │ " + strings.Repeat("y", 18) + " │")
	for _, r := range rows {
		line := "  │ " + padRight(r[0], 17) + " │ " + r[1] + " │"
		if got := displayWidth(line); got != want {
			t.Errorf("row %q renders %d columns, want %d:\n%s", r[0], got, want, line)
		}
	}
}

// TestTableRulesJunctionsAlignWithRows pins the grid geometry: every
// horizontal rule produced by tableRule must be exactly as wide as a data
// row AND its junction glyphs must sit on precisely the columns where rows
// print their │ walls. A mismatch here is the "not fully connected" table
// bug (junction one or two columns left of the wall).
func TestTableRulesJunctionsAlignWithRows(t *testing.T) {
	const (
		leftWidth  = 17 // PrintStatsTable's L
		rightWidth = 18 // PrintStatsTable's R
	)

	dataRow := "  │ " + padRight("Metric", leftWidth) + " │ " + padLeft("Value", rightWidth) + " │"

	wallCols := runeIndexes(dataRow, '│')
	if len(wallCols) != 3 {
		t.Fatalf("expected 3 walls in data row, found %d in %q", len(wallCols), dataRow)
	}

	for _, tc := range []struct {
		name string
		rule string
	}{
		{"top", tableRule("┌", "┬", "┐", leftWidth, rightWidth)},
		{"middle", tableRule("├", "┼", "┤", leftWidth, rightWidth)},
		{"bottom", tableRule("└", "┴", "┘", leftWidth, rightWidth)},
	} {
		runes := []rune(tc.rule)
		junctions := make([]int, 0, 3)
		for i, c := range runes {
			if c != '─' && c != ' ' {
				junctions = append(junctions, i)
			}
		}
		if len(junctions) != 3 {
			t.Fatalf("%s rule has %d junction glyphs, want 3: %q", tc.name, len(junctions), tc.rule)
		}
		if got := displayWidth(tc.rule); got != displayWidth(dataRow) {
			t.Errorf("%s rule renders %d columns, row renders %d",
				tc.name, got, displayWidth(dataRow))
		}
		for k, col := range junctions {
			if col != wallCols[k] {
				t.Errorf("%s rule junction %d at column %d, wall at %d — grid not connected:\n%s\n%s",
					tc.name, k, col, wallCols[k], tc.rule, dataRow)
			}
		}
	}
}

// runeIndexes returns the rune indices of target within s.
func runeIndexes(s string, target rune) []int {
	var idx []int
	for i, c := range []rune(s) {
		if c == target {
			idx = append(idx, i)
		}
	}
	return idx
}

// TestFitCellNeverBreaksProperty fuzzes mixed-script content against random
// widths: output must always render at exactly the requested columns and
// never contain cut runes.
func TestFitCellNeverBreaksProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	alphabet := []string{
		"a", "b", "z", "0", "-", ".", "/", ":",
		"🎉", "🚀", "⚡", "🔥", // wide emoji
		"é", "ñ", "ü", // narrow accented
	}
	for i := 0; i < 2000; i++ {
		var b strings.Builder
		n := rng.Intn(30)
		for j := 0; j < n; j++ {
			b.WriteString(alphabet[rng.Intn(len(alphabet))])
		}
		s := b.String()
		w := rng.Intn(25) + 1

		got := fitCell(s, w)
		if dw := displayWidth(got); dw != w {
			t.Fatalf("iter %d: fitCell(%q, %d) = %q renders %d columns",
				i, s, w, got, dw)
		}
	}
}
