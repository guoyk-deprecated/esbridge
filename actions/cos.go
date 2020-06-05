package actions

import (
	"context"
	"github.com/guoyk93/esbridge/pkg/exporter"
	"github.com/tencentyun/cos-go-sdk-v5"
	"log"
	"path"
	"strings"
)

func COSSearch(clientCOS *cos.Client, keyword string) (err error) {
	log.Printf("在腾讯云存储搜索: %s", keyword)
	var marker string
	var res *cos.BucketGetResult
	for {
		if res, _, err = clientCOS.Bucket.Get(context.Background(), &cos.BucketGetOptions{
			Marker:  marker,
			MaxKeys: 5,
		}); err != nil {
			return
		}
		for _, o := range res.Contents {
			if strings.HasSuffix(o.Key, exporter.Ext) {
				p := strings.TrimPrefix(strings.TrimSuffix(o.Key, exporter.Ext), "/")
				dir, file := path.Split(p)
				log.Printf("找到 INDEX = %s, PROJECT = %s", dir, file)
			}
		}
		if res.IsTruncated {
			marker = res.NextMarker
		} else {
			return
		}
	}
}
