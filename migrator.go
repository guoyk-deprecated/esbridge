package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/guoyk93/conc"
	"github.com/guoyk93/iocount"
	"github.com/guoyk93/logutil"
	gzip "github.com/klauspost/pgzip"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

const (
	ExtNDJSON           = ".ndjson"
	ExtCompressedNDJSON = ".ndjson.gz"
)

type ExporterOptions struct {
	// ESClient
	ESClient *elastic.Client
	// COSClient
	COSClient *cos.Client
	// local directory
	Dir string
	// index to export
	Index string
	// bulk
	Bulk int
	// concurrency
	Concurrency int
	// compress level
	CompressLevel int
}

type Exporter interface {
	conc.Task
}

const (
	keyProjects = "projects"
	keyProject  = "project"
)

var (
	newLine = []byte{'\n'}
)

type exporter struct {
	ExporterOptions
}

func (e *exporter) Run(ctx context.Context) (err error) {
	var projects []string
	if projects, err = e.collectProjects(ctx); err != nil {
		return
	}

	log.Printf("projects: %s", strings.Join(projects, ", "))

	tokens := make(chan bool, e.Concurrency)
	for i := 0; i < e.Concurrency; i++ {
		tokens <- true
	}

	results := make(chan error)

	exportCtx, exportCancel := context.WithCancel(ctx)
	for _, project := range projects {
		go e.exportProject(exportCtx, project, tokens, results)
	}

	p := logutil.NewProgress(logutil.LoggerFunc(log.Printf), "export all")
	p.SetTotal(int64(len(projects)))

	for i := 0; i < len(projects); i++ {
		p.Incr()
		err1 := <-results
		if err1 != nil {
			exportCancel()
			err = err1
			return
		}
	}

	return
}

func (e *exporter) deleteRawData(ctx context.Context, project string) (err error) {
	err = os.Remove(filepath.Join(e.Dir, project+ExtNDJSON))
	return
}

func (e *exporter) compressRawData(ctx context.Context, project string) (err error) {
	fname,
	fnameZ := filepath.Join(e.Dir, project+ExtNDJSON),
		filepath.Join(e.Dir, project+ExtCompressedNDJSON)

	var fi os.FileInfo
	if fi, err = os.Stat(fname); err != nil {
		return
	}

	log.Printf("raw size: %s %dmb", project, fi.Size()/1024/1024)

	// reader - raw file
	var r *os.File
	if r, err = os.Open(fname); err != nil {
		return
	}
	defer r.Close()

	// reader - raw file with iocount
	cr := iocount.NewReader(r)

	// writer - dest file
	var w *os.File
	if w, err = os.OpenFile(fnameZ, os.O_RDWR|os.O_CREATE, 0640); err != nil {
		return
	}
	defer w.Close()

	// writer - gzip writer
	var zw *gzip.Writer
	if zw, err = gzip.NewWriterLevel(w, e.CompressLevel); err != nil {
		return
	}
	defer zw.Close()

	p := logutil.NewProgress(logutil.LoggerFunc(log.Printf), fmt.Sprintf("compressing %s", project))
	p.SetTotal(fi.Size())
	ctxP, cancelP := context.WithCancel(context.Background())
	defer cancelP()

	go func() {
		t := time.NewTicker(time.Second * 3)
		for {
			select {
			case <-t.C:
				p.SetCount(cr.ReadCount())
			case <-ctxP.Done():
				return
			}
		}
	}()

	if _, err = io.Copy(zw, cr); err != nil {
		return
	}

	if fi, err = os.Stat(fnameZ); err != nil {
		return
	}

	log.Printf("compressed size: %s %dmb", project, fi.Size()/1024/1024)
	return
}

func (e *exporter) exportRawData(ctx context.Context, project string) (err error) {
	// create ndjson file
	var file *os.File
	if file, err = os.OpenFile(filepath.Join(e.Dir, project+ExtNDJSON), os.O_CREATE|os.O_RDWR, 0640); err != nil {
		return
	}
	defer file.Close()
	// create scroll service
	scroll := e.ESClient.Scroll(e.Index).Pretty(false).Scroll("5m").Query(
		elastic.NewTermQuery(keyProject, project),
	).Size(e.Bulk)
	// progress
	p := logutil.NewProgress(logutil.LoggerFunc(log.Printf), fmt.Sprintf("export raw %s", project))
	// scroll all documents within project
	var res *elastic.SearchResult
	for {
		if res, err = scroll.Do(ctx); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		if err = ctx.Err(); err != nil {
			return
		}

		p.SetTotal(res.Hits.TotalHits)
		p.Add(int64(len(res.Hits.Hits)))

		for _, hit := range res.Hits.Hits {
			if (hit.Source) == nil {
				continue
			}
			buf := *hit.Source
			if _, err = file.Write(buf); err != nil {
				return
			}
			if _, err = file.Write(newLine); err != nil {
				return
			}
		}
	}
	return
}

func (e *exporter) logMemUsageAndGC(label string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("%s mem: %v mb / %v mb",
		label,
		m.Alloc/1024/1024,
		m.Sys/1024/1024,
	)
	debug.FreeOSMemory()
}

func (e *exporter) exportProject(ctx context.Context, project string, tokens chan bool, results chan error) {
	var err error
	defer func() { results <- err }()

	// borrow and return token for concurrency control
	<-tokens
	defer func() { tokens <- true }()

	log.Printf("exporting: %s", project)
	if err = e.exportRawData(ctx, project); err != nil {
		return
	}
	e.logMemUsageAndGC("export")
	log.Printf("exported: %s", project)
	log.Printf("compressing: %s", project)
	if err = e.compressRawData(ctx, project); err != nil {
		return
	}
	e.logMemUsageAndGC("compress")
	log.Printf("compressed: %s", project)
	if err = e.deleteRawData(ctx, project); err != nil {
		return
	}
}

func (e *exporter) collectProjects(ctx context.Context) (projects []string, err error) {
	var res *elastic.SearchResult
	if res, err = e.ESClient.Search(e.Index).Aggregation(
		keyProjects,
		elastic.NewTermsAggregation().Field(keyProject).Size(99999),
	).Size(0).Do(ctx); err != nil {
		return
	}
	termAgg, ok := res.Aggregations.Terms(keyProjects)
	if !ok {
		err = errors.New("bad response for aggregation")
		return
	}
	if termAgg.SumOfOtherDocCount > 0 {
		err = errors.New("'sum_other_doc_count' is greater than 0, bad choose for 'project'")
		return
	}
	for _, bucket := range termAgg.Buckets {
		key, ok := bucket.Key.(string)
		if !ok {
			err = errors.New("non-string 'key' found in aggregation, bad choose of 'project'")
			return
		}
		projects = append(projects, key)
	}
	return
}

func NewExporter(opts ExporterOptions) Exporter {
	if opts.Index == "" {
		panic("missing opts.Index")
	}
	if opts.Bulk <= 0 {
		opts.Bulk = 2000
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 3
	}
	return &exporter{
		ExporterOptions: opts,
	}
}
