//go:build linux

package storm

import (
	"os"
	"strconv"
)

func openProcStat() (*os.File, error) {
	return os.Open("/proc/self/stat")
}

func sampleFDs() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(entries)
}

func GetMaxFDs() int {
	out, err := os.ReadFile("/proc/self/limits")
	if err != nil {
		return 1024
	}
	for _, line := range splitLines(string(out)) {
		if len(line) > 0 && line[0] == 'M' && containsStr(line, "Max open files") {
			parts := splitFields(line)
			if len(parts) >= 4 {
				v, err := strconv.Atoi(parts[3])
				if err == nil {
					return v
				}
			}
		}
	}
	return 1024
}

func splitLines(s string) []string {
	return splitBy(s, '\n')
}

func splitFields(s string) []string {
	return splitBy(s, ' ')
}

func splitBy(s string, sep byte) []string {
	var result []string
	cur := ""
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			if cur != "" {
				result = append(result, cur)
				cur = ""
			}
		} else {
			cur += string(s[i])
		}
	}
	if cur != "" {
		result = append(result, cur)
	}
	return result
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
