package tasks

import (
	"log"
	"runtime"
	"runtime/debug"
)

func PrintMemUsageAndGC(label string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("%s 内存占用: %vmb / %vmb",
		label,
		m.Alloc/1024/1024,
		m.Sys/1024/1024,
	)
	runtime.GC()
	debug.FreeOSMemory()
}
