package main

import (
	"flag"
	"github.com/guoyk93/esbridge/actions"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"log"
	"net/http"
	"net/url"
	"os"
)

var (
	conf Conf

	optConf     string
	optMigrate  string
	optRestore  string
	optSearch   string
	optNoDelete bool
)

func load() (err error) {
	flag.StringVar(&optConf, "conf", "/etc/esbridge.yml", "配置文件")
	flag.StringVar(&optMigrate, "migrate", "", "要迁移的离线索引, ")
	flag.StringVar(&optRestore, "restore", "", "要恢复的离线索引, 格式为 INDEX/PROJECT")
	flag.StringVar(&optSearch, "search", "", "要搜索的关键字")
	flag.BoolVar(&optNoDelete, "no-delete", false, "迁移时不删除索引，仅用于测试")
	flag.Parse()

	if conf, err = LoadConf(optConf); err != nil {
		return
	}
	return
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
		if err = actions.WorkspaceSetup(conf.Workspace); err != nil {
			return
		}
		defer actions.WorkspaceClear(conf.Workspace)

		if err = actions.ESOpenIndex(clientES, optMigrate); err != nil {
			return
		}
		if err = actions.ESExportToWorkspace(clientES, conf.Workspace, optMigrate); err != nil {
			return
		}
		if err = actions.WorkspaceUploadToCOS(conf.Workspace, clientCOS, optMigrate); err != nil {
			return
		}
		if !optNoDelete {
			if err = actions.ESDeleteIndex(clientES, optMigrate); err != nil {
				return
			}
		}
	case optRestore != "":
	case optSearch != "":
		if err = actions.COSSearch(clientCOS, optSearch); err != nil {
			return
		}
	}
}
