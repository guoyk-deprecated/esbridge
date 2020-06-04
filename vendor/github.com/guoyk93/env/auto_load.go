package env

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	cfs := CandidateFiles()
	for _, cf := range cfs {
		if err := LoadFile(cf); err == nil {
			break
		}
	}
}

func CandidateFiles() []string {
	var out []string
	p := strings.ToLower(strings.TrimSpace(os.Getenv("PROFILE")))
	if len(p) > 0 {
		f := fmt.Sprintf(".%s.env", p)
		out = append(out, f, filepath.Join("config", f), filepath.Join("conf", f))
	}
	out = append(out, ".env", filepath.Join("config", ".env"), filepath.Join("conf", ".env"))
	return out
}
