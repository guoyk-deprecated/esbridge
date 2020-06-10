package tasks

import (
	"context"
	"fmt"
	"github.com/guoyk93/conc"
	"github.com/guoyk93/esexporter"
	"github.com/guoyk93/logutil"
	"github.com/klauspost/compress/gzip"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
)

const (
	ExtCompressedNDJSON = ".ndjson.gz"
)

var (
	newLine = []byte{'\n'}
)

type ProjectMigrateOptions struct {
	IndexMigrateOptions
	Project string
}

func (opts ProjectMigrateOptions) FilenameLocal() string {
	return filepath.Join(opts.Workspace(), opts.Project+ExtCompressedNDJSON)
}

func (opts ProjectMigrateOptions) FilenameRemote() string {
	return opts.Index + "/" + opts.Project + ExtCompressedNDJSON
}

func ProjectMigrate(opts ProjectMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) error {
		res, err := opts.COSClient.Object.Head(ctx, opts.FilenameRemote(), nil)
		if err == nil && res.StatusCode == http.StatusOK {
			log.Printf("索引/项目已经存在: %s/%s", opts.Index, opts.Project)
			return nil
		}
		return conc.Serial(
			ProjectExportCompressedData(opts),
			ProjectUploadCompressedData(opts),
		).Do(ctx)
	})
}

func ProjectExportCompressedData(opts ProjectMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		title := fmt.Sprintf("导出项目数据到本地: %s/%s", opts.Index, opts.Project)
		log.Println(title)

		if err = os.MkdirAll(opts.Workspace(), 0755); err != nil {
			return
		}

		var zf *os.File
		if zf, err = os.OpenFile(opts.FilenameLocal(), os.O_CREATE|os.O_RDWR, 0640); err != nil {
			return
		}
		defer zf.Close()

		var zw *gzip.Writer
		if zw, err = gzip.NewWriterLevel(zf, gzip.BestCompression); err != nil {
			return
		}
		defer zw.Close()

		prg := logutil.NewProgress(logutil.LoggerFunc(log.Printf), title)

		if err = esexporter.New(opts.ESClient, esexporter.Options{
			Index:  opts.Index,
			Query:  elastic.NewTermQuery("project", opts.Project),
			Type:   "_doc",
			Scroll: "3m",
			Size:   int64(opts.Bulk),
		}, func(buf []byte, id int64, total int64) (err error) {
			prg.SetTotal(total)
			prg.SetCount(id + 1)
			if _, err = zw.Write(buf); err != nil {
				return
			}
			if _, err = zw.Write(newLine); err != nil {
				return
			}
			return
		}).Do(ctx); err != nil {
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
			opts.FilenameRemote(),
			opts.FilenameLocal(),
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
		if err = os.Remove(opts.FilenameLocal()); err != nil {
			return
		}
		return
	})
}
