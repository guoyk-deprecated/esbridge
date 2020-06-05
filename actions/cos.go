package actions

import (
	"context"
	"github.com/guoyk93/esbridge/pkg/exporter"
	"github.com/tencentyun/cos-go-sdk-v5"
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
			if !strings.HasSuffix(o.Key, exporter.Ext) {
				log.Printf("发现未知文件: %s", o.Key)
				continue
			}
			p := strings.TrimPrefix(strings.TrimSuffix(o.Key, exporter.Ext), "/")
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
