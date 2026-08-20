package main

import (
	"fmt"

	"github.com/fatih/color"
)

func printBanner() {
	bold := color.New(color.FgWhite, color.Bold).SprintFunc()
	brightRed := color.New(color.FgHiRed).SprintFunc()

	fmt.Println()
	fmt.Printf("  %s\n", bold("╔═══════════════════════════════════════╗"))
	fmt.Printf("  ║          %s go-storm %s          ║\n", brightRed("⚡"), bold(version))
	fmt.Printf("  %s\n", bold("║   The Load Tester That Tells Truth   ║"))
	fmt.Printf("  %s\n", bold("╚═══════════════════════════════════════╝"))
	fmt.Println()
}
