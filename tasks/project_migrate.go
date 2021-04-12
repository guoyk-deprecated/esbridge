package tasks

import (
	"context"
	"fmt"
	"github.com/guoyk93/conc"
	"github.com/guoyk93/esexporter"
	"github.com/guoyk93/logutil"
	gzip "github.com/klauspost/pgzip"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
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

func (opts ProjectMigrateOptions) FilenameCompressedLocal() string {
	return filepath.Join(opts.Workspace(), opts.Project+ExtCompressedNDJSON)
}

func (opts ProjectMigrateOptions) FilenameUncompressedLocal() string {
	return filepath.Join(opts.Workspace(), opts.Project+ExtNDJSON)
}

func (opts ProjectMigrateOptions) FilenameCompressedRemote() string {
	return opts.Index + "/" + opts.Project + ExtCompressedNDJSON
}

func ProjectMigrate(opts ProjectMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) error {
		res, err := opts.COSClient.Object.Head(ctx, opts.FilenameCompressedRemote(), nil)
		if err == nil && res.StatusCode == http.StatusOK {
			log.Printf("索引/项目已经存在: %s/%s", opts.Index, opts.Project)
			return nil
		}
		return conc.Serial(
			ProjectExportUncompressedData(opts),
			ProjectCompressData(opts),
			ProjectUploadCompressedData(opts),
		).Do(ctx)
	})
}

func ProjectExportUncompressedData(opts ProjectMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		title := fmt.Sprintf("导出项目数据到本地: %s/%s", opts.Index, opts.Project)
		log.Println(title)

		if err = os.MkdirAll(opts.Workspace(), 0755); err != nil {
			return
		}

		var rf *os.File
		if rf, err = os.OpenFile(opts.FilenameUncompressedLocal(), os.O_CREATE|os.O_RDWR, 0640); err != nil {
			return
		}
		defer rf.Close()

		prg := logutil.NewProgress(logutil.LoggerFunc(log.Printf), title)

		task := conc.TaskFunc(func(ctx context.Context) error {
			return esexporter.New(opts.ESClient, esexporter.Options{
				Index:     opts.Index,
				Query:     elastic.NewTermQuery("project", opts.Project),
				Type:      "_doc",
				Scroll:    "10m",
				BatchSize: int64(opts.BatchSize),
			}, func(buf []byte, id int64, total int64) (err error) {
				prg.SetTotal(total)
				prg.SetCount(id + 1)
				if _, err = rf.Write(buf); err != nil {
					return
				}
				if _, err = rf.Write(newLine); err != nil {
					return
				}
				return
			}).Do(ctx)
		})

		if err = task.Do(ctx); err != nil {
			return
		}

		PrintMemUsageAndGC(title)

		return
	})
}

func ProjectCompressData(opts ProjectMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		title := fmt.Sprintf("压缩数据: %s/%s", opts.Index, opts.Project)
		log.Println(title)

		var rf *os.File
		if rf, err = os.OpenFile(opts.FilenameUncompressedLocal(), os.O_RDONLY, 0640); err != nil {
			return
		}
		defer rf.Close()

		var zf *os.File
		if zf, err = os.OpenFile(opts.FilenameCompressedLocal(), os.O_CREATE|os.O_RDWR, 0640); err != nil {
			return
		}
		defer zf.Close()

		zw := gzip.NewWriter(zf)

		if _, err = io.Copy(zw, rf); err != nil {
			return
		}

		if err = zw.Close(); err != nil {
			return
		}

		if err = zf.Close(); err != nil {
			return
		}

		if err = rf.Close(); err != nil {
			return
		}

		if err = os.Remove(opts.FilenameUncompressedLocal()); err != nil {
			return
		}

		PrintMemUsageAndGC(title)
		return
	})
}

func ProjectUploadCompressedData(opts ProjectMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		log.Printf("上传本地文件: %s/%s", opts.Index, opts.Project)
		if _, _, err = opts.COSClient.Object.Upload(ctx,
			opts.FilenameCompressedRemote(),
			opts.FilenameCompressedLocal(),
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
		log.Printf("删除本地文件: %s/%s", opts.Index, opts.Project)
		if err = os.Remove(opts.FilenameCompressedLocal()); err != nil {
			return
		}
		return
	})
}
