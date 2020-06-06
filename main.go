package main

import (
	"errors"
	"flag"
	"github.com/guoyk93/esbridge/actions"
	gzip "github.com/klauspost/pgzip"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "net/http/pprof"
)

var (
	conf Conf

	optConf     string
	optMigrate  string
	optRestore  string
	optSearch   string
	optNoDelete bool

	optBestCompression bool
	optBestSpeed       bool
)

func load() (err error) {
	flag.StringVar(&optConf, "conf", "/etc/esbridge.yml", "配置文件")
	flag.StringVar(&optMigrate, "migrate", "", "要迁移的离线索引, ")
	flag.StringVar(&optRestore, "restore", "", "要恢复的离线索引, 格式为 INDEX/PROJECT")
	flag.StringVar(&optSearch, "search", "", "要搜索的关键字")
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
	if clientES, err = elastic.NewClient(elastic.SetURL(conf.Elasticsearch.URL)); err != nil {
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

		workspace := filepath.Join(conf.Workspace, index)

		if err = actions.WorkspaceSetup(workspace); err != nil {
			return
		}
		defer actions.WorkspaceClear(workspace)

		if err = actions.ESOpenIndex(clientES, index); err != nil {
			return
		}

		level := gzip.DefaultCompression

		if optBestCompression {
			level = gzip.BestCompression
		} else if optBestSpeed {
			level = gzip.BestSpeed
		}

		if err = actions.ESExportToWorkspace(clientES, workspace, index); err != nil {
			return
		}

		if err = actions.WorkspaceGzipAll(workspace, level); err != nil {
			return
		}

		if err = actions.WorkspaceUploadToCOS(workspace, clientCOS, index); err != nil {
			return
		}

		if !optNoDelete {
			if err = actions.ESDeleteIndex(clientES, index); err != nil {
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

		if err = actions.COSCheckFile(clientCOS, index, project); err != nil {
			return
		}

		if err = actions.ESTouchIndex(clientES, index); err != nil {
			return
		}

		if err = actions.ESDisableRefresh(clientES, index); err != nil {
			return
		}
		defer actions.ESEnableRefresh(clientES, index)

		if err = actions.COSImportToES(clientCOS, index, project, clientES); err != nil {
			return
		}

	case optSearch != "":
		if err = actions.COSSearch(clientCOS, optSearch); err != nil {
			return
		}
	}
}
