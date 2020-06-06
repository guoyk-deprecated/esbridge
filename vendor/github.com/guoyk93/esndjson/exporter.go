package esndjson

import (
	"context"
	"errors"
	"fmt"
	"github.com/guoyk93/progress"
	gzip "github.com/klauspost/pgzip"
	"github.com/olivere/elastic"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
)

const (
	ExtNDJSON        = ".ndjson"
	ExtNDJSONGzipped = ".ndjson.gz"
)

type Logger func(layout string, items ...interface{})

type ExporterOptions struct {
	// elasticsearch url
	URL string
	// elasticsearch sniff
	Sniff bool
	// local directory
	Dir string
	// index to exportSection
	Index string
	// section key for term aggregation and term search
	SectionKey string
	// concurrency
	Concurrency int
	// compress level
	CompressLevel int
	// logger
	Logger Logger
}

type Exporter interface {
	Run(ctx context.Context) (err error)
}

const (
	keySections = "sections"
)

var (
	newLine = []byte{'\n'}

	discardLogger Logger = func(layout string, items ...interface{}) {}
)

type exporter struct {
	c    *elastic.Client
	opts ExporterOptions
}

func (e *exporter) Run(ctx context.Context) (err error) {
	var sections []string
	if sections, err = e.collectAggregationValues(ctx); err != nil {
		return
	}

	e.opts.Logger("found sections: %s = [%s]", e.opts.SectionKey, strings.Join(sections, ", "))

	tokens := make(chan bool, e.opts.Concurrency)
	for i := 0; i < e.opts.Concurrency; i++ {
		tokens <- true
	}

	results := make(chan error)

	exportCtx, exportCancel := context.WithCancel(ctx)
	for _, section := range sections {
		go e.exportSection(exportCtx, section, tokens, results)
	}

	p := progress.NewProgress(int64(len(sections)), "export sections", progress.Logger(e.opts.Logger))

	for i := 0; i < len(sections); i++ {
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

func (e *exporter) deleteRawData(ctx context.Context, section string) (err error) {
	err = os.Remove(filepath.Join(e.opts.Dir, section+ExtNDJSON))
	return
}

func (e *exporter) compressRawData(ctx context.Context, section string) (err error) {
	var file *os.File
	if file, err = os.Open(filepath.Join(e.opts.Dir, section+ExtNDJSON)); err != nil {
		return
	}
	defer file.Close()

	var zfile *os.File
	if zfile, err = os.OpenFile(filepath.Join(e.opts.Dir, section+ExtNDJSONGzipped), os.O_RDWR|os.O_CREATE, 0640); err != nil {
		return
	}
	defer zfile.Close()

	var zw *gzip.Writer
	if zw, err = gzip.NewWriterLevel(zfile, e.opts.CompressLevel); err != nil {
		return
	}
	defer zw.Close()

	if _, err = io.Copy(zw, file); err != nil {
		return
	}
	return
}

func (e *exporter) exportRawData(ctx context.Context, section string) (err error) {
	// create ndjson file
	var file *os.File
	if file, err = os.OpenFile(filepath.Join(e.opts.Dir, section+ExtNDJSON), os.O_CREATE|os.O_RDWR, 0640); err != nil {
		return
	}
	defer file.Close()
	// create scroll service
	scroll := e.c.Scroll(e.opts.Index).Pretty(false).Scroll("5m").Query(
		elastic.NewTermQuery(e.opts.SectionKey, section),
	).Size(2000)
	// progress
	var p progress.Progress
	// scroll all documents within section
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
		if p == nil {
			p = progress.NewProgress(res.Hits.TotalHits, fmt.Sprintf("export raw %s", section), progress.Logger(e.opts.Logger))
		}
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

func (e *exporter) logMemoryUsageAndGC(label string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	e.opts.Logger("%s mem: %v mb / %v mb",
		label,
		m.Alloc/1024/1024,
		m.Sys/1024/1024,
	)
	debug.FreeOSMemory()
}

func (e *exporter) exportSection(ctx context.Context, section string, tokens chan bool, results chan error) {
	var err error
	defer func() { results <- err }()

	// borrow and return token for concurrency control
	<-tokens
	defer func() { tokens <- true }()

	e.opts.Logger("exporting: %s", section)
	if err = e.exportRawData(ctx, section); err != nil {
		return
	}
	e.logMemoryUsageAndGC("export")
	e.opts.Logger("exported: %s", section)
	e.opts.Logger("compressing: %s", section)
	if err = e.compressRawData(ctx, section); err != nil {
		return
	}
	e.logMemoryUsageAndGC("compress")
	e.opts.Logger("compressed: %s", section)
	if err = e.deleteRawData(ctx, section); err != nil {
		return
	}
}

func (e *exporter) collectAggregationValues(ctx context.Context) (sections []string, err error) {
	var res *elastic.SearchResult
	if res, err = e.c.Search(e.opts.Index).Aggregation(
		keySections,
		elastic.NewTermsAggregation().Field(e.opts.SectionKey).Size(99999),
	).Size(0).Do(ctx); err != nil {
		return
	}
	termAgg, ok := res.Aggregations.Terms(keySections)
	if !ok {
		err = errors.New("bad response for aggregation")
		return
	}
	if termAgg.SumOfOtherDocCount > 0 {
		err = errors.New("'sum_other_doc_count' is greater than 0, bad choose for opts.SectionKey")
		return
	}
	for _, bucket := range termAgg.Buckets {
		key, ok := bucket.Key.(string)
		if !ok {
			err = errors.New("non-string 'key' found in aggregation, bad choose of opts.SectionKey")
			return
		}
		sections = append(sections, key)
	}
	return
}

func NewExporter(opts ExporterOptions) (Exporter, error) {
	if opts.Index == "" {
		return nil, errors.New("missing opts.Index")
	}
	if opts.SectionKey == "" {
		return nil, errors.New("missing opts.SectionKey")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 3
	}
	if opts.Logger == nil {
		opts.Logger = discardLogger
	}
	var err error
	var client *elastic.Client
	if client, err = elastic.NewClient(
		elastic.SetURL(opts.URL),
		elastic.SetSniff(opts.Sniff),
	); err != nil {
		return nil, err
	}
	return &exporter{
		c:    client,
		opts: opts,
	}, nil
}
