package storm

import (
	"regexp"
	"strings"

	"github.com/rivo/uniseg"
)

// Fixed-width terminal tables break when cells are measured in BYTES while
// terminals render in DISPLAY COLUMNS: multi-byte runes corrupt mid-cut,
// emoji/CJK render two columns wide, ANSI color codes add invisible length.
// Every table cell therefore passes through fitCell, which measures and
// cuts on display width — the same discipline buildBanner uses for the box.

// ansiSGR matches ANSI SGR escape sequences (colors/styles).
var ansiSGR = regexp.MustCompile("\x1b\\[[0-9;]*[A-Za-z]")

// stripANSI removes ANSI escape sequences so measurement sees only what a
// human sees.
func stripANSI(s string) string {
	return ansiSGR.ReplaceAllString(s, "")
}

// displayWidth returns how many terminal columns s occupies when rendered —
// ANSI stripped, measured per grapheme cluster (emoji + ZWJ sequences count
// once; East Asian wide characters count twice).
func displayWidth(s string) int {
	return uniseg.StringWidth(stripANSI(s))
}

// cutToWidth returns the longest prefix of s that renders within w display
// columns, cutting only on grapheme boundaries. ANSI stripped; no marker,
// no padding — the primitive other helpers compose.
func cutToWidth(s string, w int) string {
	if w <= 0 {
		return ""
	}
	var out strings.Builder
	cur := 0
	g := uniseg.NewGraphemes(stripANSI(s))
	for g.Next() {
		gw := g.Width()
		if cur+gw > w {
			break
		}
		out.WriteString(g.Str())
		cur += gw
	}
	return out.String()
}

// fitCell fits s into exactly width display columns. Content narrower than
// the cell is padded with spaces; content wider is cut on grapheme
// boundaries to width-2 columns and marked with "..". It never panics,
// never splits a rune, and its output always renders at exactly width
// columns — the guarantee that keeps table borders aligned.
func fitCell(s string, width int) string {
	if width <= 0 {
		return ""
	}
	visible := stripANSI(s)
	if w := displayWidth(visible); w <= width {
		return visible + strings.Repeat(" ", width-w)
	}
	if width <= 2 {
		// Degenerate widths (1–2 columns): only the marker fits.
		return ".."[:width]
	}
	prefix := cutToWidth(visible, width-2)
	pad := width - displayWidth(prefix) - 2 // last wide cluster may have been skipped
	return prefix + ".." + strings.Repeat(" ", pad)
}
