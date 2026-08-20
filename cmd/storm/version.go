package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version of storm",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("go-storm version %s\n", version)
		fmt.Printf("commit:     %s\n", commit)
		fmt.Printf("built:      %s\n", buildDate)
		fmt.Printf("platform:   %s/%s\n", runtime.GOOS, runtime.GOARCH)
		fmt.Printf("go:         %s\n", runtime.Version())
	},
}
