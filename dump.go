package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func dumpESToLocal(clientES *elastic.Client, dir, index string) (err error) {
	dw := NewDumpWriter(dir)
	defer dw.Close()

	var total int64
	if total, err = clientES.Count(index).Do(context.Background()); err != nil {
		return
	}

	ss := clientES.Scroll(index).Type("_doc").Scroll("1m").Size(10000)
	defer ss.Clear(context.Background())

	p := NewProgress(total, fmt.Sprintf("export [%s]", index))

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
			p.Incr()
			if h.Source != nil {
				if err = dw.Append(*h.Source); err != nil {
					return
				}
			}
		}
	}
	return
}

func dumpLocalToCOS(dir string, clientCOS *cos.Client, index string) (err error) {
	var fis []os.FileInfo
	if fis, err = ioutil.ReadDir(dir); err != nil {
		return err
	}
	p := NewProgress(int64(len(fis)), "upload cos")
	for _, fi := range fis {
		p.Incr()
		if !strings.HasSuffix(fi.Name(), ".ndjson.gz") {
			continue
		}
		if _, _, err = clientCOS.Object.Upload(context.Background(), index+"/"+fi.Name(), filepath.Join(dir, fi.Name()), &cos.MultiUploadOptions{
			OptIni: &cos.InitiateMultipartUploadOptions{
				ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{XCosStorageClass: "STANDARD_IA"},
			},
		}); err != nil {
			return
		}
	}
	return
}
