package tasks

import (
	"context"
	"fmt"
	"github.com/guoyk93/conc"
	"github.com/guoyk93/esexporter"
	"github.com/guoyk93/iocount"
	"github.com/guoyk93/logutil"
	gzip "github.com/klauspost/pgzip"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const (
	ExtNDJSON           = ".ndjson"
	ExtCompressedNDJSON = ".ndjson.gz"
)

var (
	newLine = []byte{'\n'}
)

type ProjectMigrateOptions struct {
	IndexMigrateOptions
	Project string
}

func (opts ProjectMigrateOptions) FilenameRaw() string {
	return filepath.Join(opts.Workspace(), opts.Project+ExtNDJSON)
}

func (opts ProjectMigrateOptions) FilenameCompressed() string {
	return filepath.Join(opts.Workspace(), opts.Project+ExtCompressedNDJSON)
}

func ProjectMigrate(opts ProjectMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) error {
		res, err := opts.COSClient.Object.Head(ctx, opts.Index+"/"+opts.Project+ExtCompressedNDJSON, nil)
		if err == nil {
			buf, _ := ioutil.ReadAll(res.Body)
			log.Printf("索引/项目已经存在: %s", buf)
			return nil
		}
		return conc.Serial(
			ProjectExportRawData(opts),
			ProjectCompressData(opts),
			ProjectUploadCompressedData(opts),
		).Do(ctx)
	})
}

func ProjectExportRawData(opts ProjectMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		title := fmt.Sprintf("导出索引原始数据: %s/%s", opts.Index, opts.Project)
		log.Println(title)

		if err = os.MkdirAll(opts.Workspace(), 0755); err != nil {
			return
		}

		var f *os.File
		if f, err = os.OpenFile(opts.FilenameRaw(), os.O_CREATE|os.O_RDWR, 0640); err != nil {
			return
		}
		defer f.Close()

		prg := logutil.NewProgress(logutil.LoggerFunc(log.Printf), title)

		if err = esexporter.New(esexporter.Options{
			Client: opts.ESClient,
			Index:  opts.Index,
			Query:  elastic.NewTermQuery("project", opts.Project),
			Type:   "_doc",
			Scroll: "3m",
			Size:   int64(opts.Bulk),
		}, func(buf []byte, id int64, total int64) (err error) {
			prg.SetTotal(total)
			prg.SetCount(id + 1)
			if _, err = f.Write(buf); err != nil {
				return
			}
			if _, err = f.Write(newLine); err != nil {
				return
			}
			return
		}).Do(ctx); err != nil {
			return
		}

		scroll := opts.ESClient.Scroll(opts.Index).Pretty(false).Scroll("5m").Query(
			elastic.NewTermQuery("project", opts.Project),
		).Size(opts.Bulk)

		var res *elastic.SearchResult
		for {
			if res, err = scroll.Do(ctx); err != nil {
				if err == io.EOF {
					err = nil
				}
				break
			}

			prg.SetTotal(res.Hits.TotalHits)
			prg.Add(int64(len(res.Hits.Hits)))

			for _, hit := range res.Hits.Hits {
				if hit.Source == nil {
					continue
				}
				buf := *hit.Source
				if _, err = f.Write(buf); err != nil {
					return
				}
				if _, err = f.Write(newLine); err != nil {
					return
				}
			}
		}
		PrintMemUsageAndGC(title)
		return
	})
}

func ProjectCompressData(opts ProjectMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		title := fmt.Sprintf("压缩原始数据: %s/%s", opts.Index, opts.Project)
		log.Println(title)

		var rawInfo os.FileInfo
		if rawInfo, err = os.Stat(opts.FilenameRaw()); err != nil {
			return
		}
		log.Printf("原始数据大小: %s/%s, %dmb", opts.Index, opts.Project, rawInfo.Size()/1024/1024)

		var rawFile *os.File
		if rawFile, err = os.Open(opts.FilenameRaw()); err != nil {
			return
		}
		defer rawFile.Close()

		rawReader := iocount.NewReader(rawFile)

		var cpsFile *os.File
		if cpsFile, err = os.OpenFile(opts.FilenameCompressed(), os.O_CREATE|os.O_RDWR, 0644); err != nil {
			return
		}
		defer cpsFile.Close()

		var cpsWriter *gzip.Writer
		if cpsWriter, err = gzip.NewWriterLevel(cpsFile, opts.CompressionLevel); err != nil {
			return
		}
		defer cpsWriter.Close()

		prg := logutil.NewProgress(logutil.LoggerFunc(log.Printf), title)
		prg.SetTotal(rawInfo.Size())
		ctxP, cancelP := context.WithCancel(context.Background())
		defer cancelP()

		go func() {
			t := time.NewTicker(time.Second * 3)
			for {
				select {
				case <-t.C:
					prg.SetCount(rawReader.ReadCount())
				case <-ctxP.Done():
					return
				}
			}
		}()

		if _, err = io.Copy(cpsWriter, rawReader); err != nil {
			return
		}

		var cpsInfo os.FileInfo
		if cpsInfo, err = os.Stat(opts.FilenameCompressed()); err != nil {
			return
		}

		log.Printf("压缩后数据大小: %s/%s, %dmb", opts.Index, opts.Project, cpsInfo.Size()/1024/1024)

		PrintMemUsageAndGC(title)

		log.Printf("删除原始文件: %s/%s", opts.Index, opts.Project)
		if err = os.Remove(opts.FilenameRaw()); err != nil {
			return
		}
		return
	})
}

func ProjectUploadCompressedData(opts ProjectMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		log.Printf("上传压缩后文件: %s/%s", opts.Index, opts.Project)
		if _, _, err = opts.COSClient.Object.Upload(ctx,
			opts.Index+"/"+opts.Project+ExtCompressedNDJSON,
			opts.FilenameCompressed(),
			&cos.MultiUploadOptions{
				PartSize:       1000,
				ThreadPoolSize: runtime.NumCPU(),
				OptIni: &cos.InitiateMultipartUploadOptions{
					ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{XCosStorageClass: "STANDARD_IA"},
				},
			},
		); err != nil {
			return
		}
		log.Printf("删除压缩后文件: %s/%s", opts.Index, opts.Project)
		if err = os.Remove(opts.FilenameCompressed()); err != nil {
			return
		}
		return
	})
}
