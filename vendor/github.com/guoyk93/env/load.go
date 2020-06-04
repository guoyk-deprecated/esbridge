package env

import (
	"bufio"
	"os"
	"strings"
)

func LoadFile(filename string) (err error) {
	var f *os.File
	if f, err = os.Open(filename); err != nil {
		return
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	s.Split(bufio.ScanLines)

	for s.Scan() {
		if err = s.Err(); err != nil {
			return
		}
		line := strings.TrimSpace(s.Text())
		if strings.HasPrefix(line, "#") {
			continue
		}
		splits := strings.SplitN(line, "=", 2)
		if len(splits) != 2 {
			continue
		}
		k := strings.TrimSpace(splits[0])
		v := strings.TrimSpace(splits[1])
		if len(k) == 0 {
			continue
		}
		if err = os.Setenv(k, v); err != nil {
			return
		}
	}

	return
}
