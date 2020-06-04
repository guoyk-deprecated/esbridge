package main

import (
	"github.com/guoyk93/env"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
)

var (
	optIndex        string
	optAction       string
	optProject      string
	optWorkspace    string
	optEsURL        string
	optCosURL       string
	optCosSecretID  string
	optCosSecretKey string
)

const (
	actionDump   = "dump"
	actionLoad   = "load"
	actionSearch = "search"
)

func load() {
	env.MustStringVar(&optIndex, "INDEX", "")
	env.MustStringVar(&optAction, "ACTION", actionDump)
	env.MustStringVar(&optProject, "PROJECT", "")
	env.MustStringVar(&optWorkspace, "WORKSPACE", "")
	env.MustStringVar(&optEsURL, "ES_URL", "http://127.0.0.1:9200")
	env.MustStringVar(&optCosURL, "COS_URL", "")
	env.MustStringVar(&optCosSecretID, "COS_SECRET_ID", "")
	env.MustStringVar(&optCosSecretKey, "COS_SECRET_KEY", "")

	if optIndex == "" {
		panic("missing environment variable: INDEX")
	}
	switch optAction {
	case actionDump:
	case actionLoad:
		if optProject == "" {
			panic("missing environment variable: PROJECT")
		}
	case actionSearch:
	default:
		panic("invalid environment variable: ACTION")
	}
	if optWorkspace == "" {
		panic("missing environment variable: WORKSPACE")
	}
	if optCosURL == "" {
		panic("missing environment variable: COS_URL")
	}
	if optCosSecretID == "" {
		panic("missing environment variable: COS_SECRET_ID")
	}
	if optCosSecretKey == "" {
		panic("missing environment variable: COS_SECRET_KEY")
	}
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

	load()

	// setup workspace
	localDir := filepath.Join(optWorkspace, optIndex)
	if err = os.RemoveAll(localDir); err != nil {
		return
	}
	if err = os.MkdirAll(localDir, 0755); err != nil {
		return
	}

	// setup es
	var clientES *elastic.Client
	if clientES, err = elastic.NewClient(elastic.SetURL(optEsURL)); err != nil {
		return
	}

	// setup cos
	var clientCOS *cos.Client
	u, _ := url.Parse(optCosURL)
	b := &cos.BaseURL{BucketURL: u}
	clientCOS = cos.NewClient(b, &http.Client{Transport: &cos.AuthorizationTransport{SecretID: optCosSecretID, SecretKey: optCosSecretKey}})

	// dump or load
	switch optAction {
	case actionDump:
		if err = taskExportESToLocal(clientES, localDir, optIndex); err != nil {
			return
		}
		if err = taskExportLocalToCOS(localDir, clientCOS, optIndex); err != nil {
			return
		}
	case actionLoad:
	case actionSearch:
	}
}
