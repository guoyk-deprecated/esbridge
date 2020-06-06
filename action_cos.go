package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/guoyk93/esbridge/pkg/progress"
	"github.com/guoyk93/esndjson"
	"github.com/guoyk93/iocount"
	"github.com/klauspost/compress/gzip"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"log"
	"strings"
)

func COSSearch(clientCOS *cos.Client, keyword string) (err error) {
	log.Printf("在腾讯云存储搜索: %s", keyword)
	var marker string
	var res *cos.BucketGetResult
	for {
		if res, _, err = clientCOS.Bucket.Get(context.Background(), &cos.BucketGetOptions{
			Marker: marker,
		}); err != nil {
			return
		}
		for _, o := range res.Contents {
			if !strings.HasSuffix(o.Key, esndjson.ExtNDJSONGzipped) {
				log.Printf("发现未知文件: %s", o.Key)
				continue
			}
			p := strings.TrimPrefix(strings.TrimSuffix(o.Key, esndjson.ExtNDJSONGzipped), "/")
			if !strings.Contains(p, keyword) {
				continue
			}
			ss := strings.Split(p, "/")
			if len(ss) != 2 {
				log.Printf("发现未知文件: %s", o.Key)
				continue
			}
			log.Printf("找到 INDEX = %s, PROJECT = %s", ss[0], ss[1])
		}
		if res.IsTruncated {
			marker = res.NextMarker
		} else {
			return
		}
	}
}

func COSCheckFile(clientCOS *cos.Client, index, project string) (err error) {
	log.Printf("检查腾讯云存储文件: INDEX = %s, PROJECT = %s", index, project)
	_, err = clientCOS.Object.Head(context.Background(), index+"/"+project+esndjson.ExtNDJSONGzipped, nil)
	return
}

func COSImportToES(clientCOS *cos.Client, index, project string, clientES *elastic.Client) (err error) {
	log.Printf("从腾讯云存储恢复索引: %s (%s)", index, project)
	var res *cos.Response
	if res, err = clientCOS.Object.Get(context.Background(), index+"/"+project+esndjson.ExtNDJSONGzipped, nil); err != nil {
		return
	}
	defer res.Body.Close()

	p := progress.NewProgress(res.ContentLength, fmt.Sprintf("从腾讯云存储恢复索引: %s (%s)", index, project))

	cr := iocount.NewReader(res.Body)
	var zr *gzip.Reader
	if zr, err = gzip.NewReader(cr); err != nil {
		return
	}
	br := bufio.NewReader(zr)

	var bs *elastic.BulkService

	commit := func(force bool) (err error) {
		if bs != nil {
			if force || bs.NumberOfActions() > 10000 {
				var res *elastic.BulkResponse
				if res, err = bs.Do(context.Background()); err != nil {
					return
				}
				failed := res.Failed()
				if len(failed) > 0 {
					buf, _ := json.MarshalIndent(failed[0], "", "  ")
					err = fmt.Errorf("存在失败的索引请求: %s", string(buf))
					return
				}
			}
		}
		return
	}

	var buf []byte
	for {
		if buf, err = br.ReadBytes('\n'); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		buf = bytes.TrimSpace(buf)

		if len(buf) > 0 {
			if bs == nil {
				bs = clientES.Bulk()
			}
			bs.Add(elastic.NewBulkIndexRequest().Index(index).Type("_doc").Doc(json.RawMessage(buf)))
		}

		if err = commit(false); err != nil {
			return
		}

		p.Set(cr.ReadCount())
	}

	if err = commit(true); err != nil {
		return
	}

	return
}
