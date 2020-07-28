package main

import (
	"context"
	"errors"
	"flag"
	"github.com/guoyk93/esbridge/tasks"
	gzip "github.com/klauspost/pgzip"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	_ "net/http/pprof"
)

var (
	conf Conf

	optConf          string
	optMigrate       string
	optRestore       string
	optSearch        string
	optNoDelete      bool
	optBatchByteSize int
	optConcurrency   int
	optNeo           bool

	optBestCompression bool
	optBestSpeed       bool
)

func load() (err error) {
	flag.BoolVar(&optNeo, "neo", false, "neo")
	flag.StringVar(&optConf, "conf", "/etc/esbridge.yml", "配置文件")
	flag.StringVar(&optMigrate, "migrate", "", "要迁移的离线索引, ")
	flag.StringVar(&optRestore, "restore", "", "要恢复的离线索引, 格式为 INDEX/PROJECT")
	flag.StringVar(&optSearch, "search", "", "要搜索的关键字")
	flag.IntVar(&optBatchByteSize, "batch-byte-size", 1000000000, "导出时的每批次内存占用")
	flag.IntVar(&optConcurrency, "concurrency", 3, "导出时的并发数")
	flag.BoolVar(&optNoDelete, "no-delete", false, "迁移时不删除索引，仅用于测试")
	flag.BoolVar(&optBestCompression, "best-compression", false, "最佳压缩率")
	flag.BoolVar(&optBestSpeed, "best-speed", false, "最佳压缩速度")
	flag.Parse()

	optConf = strings.TrimSpace(optConf)
	optMigrate = strings.TrimSpace(optMigrate)
	optRestore = strings.TrimSpace(optRestore)
	optSearch = strings.TrimSpace(optSearch)

	if conf, err = LoadConf(optConf); err != nil {
		return
	}
	return
}

func checkIndex(index string) error {
	if strings.Contains(index, "*") || strings.Contains(index, "?") {
		return errors.New("不允许在索引名中包含 '*' 或者 '?'")
	}
	return nil
}

func exit(err *error) {
	if *err != nil {
		log.Printf("exited with error: %s", (*err).Error())
		os.Exit(1)
	} else {
		log.Println("exited")
	}
}

func main() {
	var err error
	defer exit(&err)

	if err = load(); err != nil {
		return
	}

	// pprof
	go func() {
		log.Print(http.ListenAndServe(conf.PProf.Bind, nil))
	}()

	// setup es
	var clientES *elastic.Client
	if clientES, err = elastic.NewClient(
		elastic.SetURL(conf.Elasticsearch.URL),
		elastic.SetGzip(false),
	); err != nil {
		return
	}

	// setup cos
	var clientCOS *cos.Client
	u, _ := url.Parse(conf.COS.URL)
	b := &cos.BaseURL{BucketURL: u}
	clientCOS = cos.NewClient(b, &http.Client{Transport: &cos.AuthorizationTransport{SecretID: conf.COS.SecretID, SecretKey: conf.COS.SecretKey}})

	switch {
	case optMigrate != "":
		index := optMigrate

		if err = checkIndex(index); err != nil {
			return
		}

		if optNeo {
			if err = tasks.IndexMigrateNeo(tasks.IndexMigrateOptions{
				ESClient:         clientES,
				COSClient:        clientCOS,
				NoDelete:         optNoDelete,
				Dir:              conf.Workspace,
				Index:            index,
				BatchByteSize:    optBatchByteSize,
				Concurrency:      optConcurrency,
				CompressionLevel: gzip.BestCompression,
			}).Do(context.Background()); err != nil {
				return
			}
		} else {
			if err = tasks.IndexMigrate(tasks.IndexMigrateOptions{
				ESClient:         clientES,
				COSClient:        clientCOS,
				NoDelete:         optNoDelete,
				Dir:              conf.Workspace,
				Index:            index,
				BatchByteSize:    optBatchByteSize,
				Concurrency:      optConcurrency,
				CompressionLevel: gzip.BestCompression,
			}).Do(context.Background()); err != nil {
				return
			}
		}

	case optRestore != "":
		ss := strings.Split(optRestore, "/")
		if len(ss) != 2 {
			err = errors.New("参数错误")
			return
		}
		index, project := strings.TrimSpace(ss[0]), strings.TrimSpace(ss[1])
		if index == "" || project == "" {
			err = errors.New("参数缺失")
			return
		}

		if err = checkIndex(index); err != nil {
			return
		}

		if err = COSCheckFile(clientCOS, index, project); err != nil {
			return
		}

		if err = ElasticsearchTouchIndex(clientES, index); err != nil {
			return
		}

		if err = ElasticsearchDisableRefresh(clientES, index); err != nil {
			return
		}
		defer ElasticsearchEnableRefresh(clientES, index)

		if err = COSImportToES(clientCOS, index, project, clientES); err != nil {
			return
		}

	case optSearch != "":
		if err = COSSearch(clientCOS, optSearch); err != nil {
			return
		}
	}
}
