package main

import (
	"context"
	"github.com/guoyk93/env"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"log"
	"os"
	"path/filepath"
)

var (
	optIndex     string
	optAction    string
	optWorkspace string
	optEsURL     string
	/*
		optCosURL       string
		optCosSecretID  string
		optCosSecretKey string
	*/
)

const (
	actionDump = "dump"
	actionLoad = "load"
)

func load() {
	env.MustStringVar(&optIndex, "INDEX", "")
	env.MustStringVar(&optAction, "ACTION", actionDump)
	env.MustStringVar(&optWorkspace, "WORKSPACE", "")
	env.MustStringVar(&optEsURL, "ES_URL", "http://127.0.0.1:9200")
	/*
		env.MustStringVar(&optCosURL, "COS_URL", "")
		env.MustStringVar(&optCosSecretID, "COS_SECRET_ID", "")
		env.MustStringVar(&optCosSecretKey, "COS_SECRET_KEY", "")
	*/

	if optIndex == "" {
		panic("missing environment variable: INDEX")
	}
	switch optAction {
	case actionDump:
	case actionLoad:
	default:
		panic("invalid environment variable: ACTION")
	}
	if optWorkspace == "" {
		panic("missing environment variable: WORKSPACE")
	}
	/*
		if optCosURL == "" {
			panic("missing environment variable: COS_URL")
		}
		if optCosSecretID == "" {
			panic("missing environment variable: COS_SECRET_ID")
		}
		if optCosSecretKey == "" {
			panic("missing environment variable: COS_SECRET_KEY")
		}
	*/
}

func exit(err *error) {
	if *err != nil {
		log.Printf("exited with error: %s", (*err).Error())
		os.Exit(1)
	} else {
		log.Println("exited")
	}
}

var (
	clientES  *elastic.Client
	clientCOS *cos.Client

	dirWorkspace string
)

func main() {
	var err error
	defer exit(&err)

	load()

	// setup workspace
	dirWorkspace = filepath.Join(optWorkspace, optIndex)
	if err = os.RemoveAll(dirWorkspace); err != nil {
		return
	}
	if err = os.MkdirAll(dirWorkspace, 0755); err != nil {
		return
	}

	// setup es
	if clientES, err = elastic.NewClient(elastic.SetURL(optEsURL)); err != nil {
		return
	}

	/*
		// setup cos
		u, _ := url.Parse(optCosURL)
		b := &cos.BaseURL{BucketURL: u}
		clientCOS = cos.NewClient(b, &http.Client{Transport: &cos.AuthorizationTransport{SecretID: optCosSecretID, SecretKey: optCosSecretKey}})
	*/

	// dump or load
	switch optAction {
	case actionDump:
		err = mainDump()
	case actionLoad:
		err = mainLoad()
	}
}

func mainDump() (err error) {
	if err = mainDumpToFile(); err != nil {
		return
	}
	return
}

func mainDumpToFile() (err error) {
	dw := NewDumpWriter(dirWorkspace)
	defer dw.Close()

	ss := clientES.Scroll(optIndex).Type("_doc").Scroll("1m").Size(1000)
	defer ss.Clear(context.Background())
	for {
		var res *elastic.SearchResult
		if res, err = ss.Do(context.Background()); err != nil {
			if err == io.EOF {
				err = nil
				break
			} else {
				return
			}
		}
		for _, h := range res.Hits.Hits {
			if h.Source != nil {
				if err = dw.Append(*h.Source); err != nil {
					return
				}
			}
		}
	}
	return
}

func mainLoad() (err error) {
	return
}
