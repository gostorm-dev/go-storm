package main

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
)

func runeCellWidth(r rune) int {
	if r == '⚡' {
		return 2
	}
	return 1
}

func displayWidth(s string) int {
	width := 0
	inEscape := false
	for _, r := range s {
		switch {
		case inEscape:
			if r == 'm' {
				inEscape = false
			}
		case r == '\x1b':
			inEscape = true
		default:
			width += runeCellWidth(r)
		}
	}
	return width
}

// bannerInnerPadding is the horizontal breathing room added around the
// widest content line when sizing the box interior.
const bannerInnerPadding = 4

// buildBanner renders the brand box sized to its content. Every returned
// line has an identical display width, so the walls always connect.
func buildBanner() []string {
	title := fmt.Sprintf("go-storm %s", version)
	tagline := "The Load Tester That Tells Truth"

	rows := []string{
		"⚡ " + title,
		tagline,
	}

	inner := 0
	for _, row := range rows {
		if w := displayWidth(row); w > inner {
			inner = w
		}
	}
	inner += bannerInnerPadding

	line := func(content string) string {
		pad := inner - 2 - displayWidth(content)
		return "║ " + content + strings.Repeat(" ", pad) + " ║"
	}
	top := "╔" + strings.Repeat("═", inner) + "╗"
	bottom := "╚" + strings.Repeat("═", inner) + "╝"

	bold := color.New(color.FgWhite, color.Bold).SprintFunc()
	lightning := color.New(color.FgHiRed).SprintFunc()

	// Colors go on last: layout above measured plain text only.
	box := []string{
		top,
		line(lightning("⚡") + " " + bold(title)),
		line(bold(tagline)),
		bottom,
	}

	indented := make([]string, 0, len(box)+2)
	indented = append(indented, "")
	for _, l := range box {
		indented = append(indented, "  "+l)
	}
	indented = append(indented, "")
	return indented
}

func printBanner() {
	for _, line := range buildBanner() {
		fmt.Println(line)
	}
}
