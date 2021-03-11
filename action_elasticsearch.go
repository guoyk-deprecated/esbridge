package main

import (
	"context"
	"github.com/olivere/elastic"
	"log"
)

type M map[string]interface{}

func ElasticsearchTouchIndex(clientES *elastic.Client, index string) (err error) {
	log.Printf("确保索引存在: %s", index)
	var ok bool
	if ok, err = clientES.IndexExists(index).Do(context.Background()); err != nil {
		return
	}
	if !ok {
		if _, err = clientES.CreateIndex(index).BodyJson(M{
			"settings": M{
				"index": M{
					"number_of_replicas": "0",
					"number_of_shards":   "6",
					"routing": M{
						"allocation": M{
							"exclude": M{
								"disktype": nil,
							},
							"require": M{
								"disktype": "hdd",
							},
						},
					},
				},
			},
		}).Do(context.Background()); err != nil {
			return
		}
	}
	return
}

func ElasticsearchTuneForRecoveryStart(clientES *elastic.Client, index string) (err error) {
	log.Printf("调整索引设置，为写入大量数据做准备: %s", index)
	_, err = clientES.IndexPutSettings(index).FlatSettings(true).BodyJson(M{
		"index.routing.allocation.exclude.disktype": nil,
		"index.routing.allocation.require.disktype": "hdd",
		"index.refresh_interval":                    "1m",
	}).Do(context.Background())
	return
}

func ElasticsearchTuneForRecoveryEnd(clientES *elastic.Client, index string) (err error) {
	log.Printf("恢复索引设置: %s", index)
	_, err = clientES.IndexPutSettings(index).FlatSettings(true).BodyJson(M{
		"index.routing.allocation.exclude.disktype": nil,
		"index.routing.allocation.require.disktype": "hdd",
		"index.refresh_interval":                    "10s",
	}).Do(context.Background())
	return
}
