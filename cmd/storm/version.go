package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/gostorm-dev/go-storm/internal/buildinfo"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version of storm",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("go-storm version %s\n", buildinfo.Version)
		fmt.Printf("commit:     %s\n", buildinfo.Commit)
		fmt.Printf("built:      %s\n", buildinfo.Date)
		fmt.Printf("platform:   %s/%s\n", runtime.GOOS, runtime.GOARCH)
		fmt.Printf("go:         %s\n", runtime.Version())
	},
}
