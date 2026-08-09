package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var version = "0.1.0"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version of storm",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("go-storm version %s\n", version)
		fmt.Printf("platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		fmt.Printf("go: %s\n", runtime.Version())
	},
}
