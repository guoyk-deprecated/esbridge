package main

import (
	"context"
	"github.com/olivere/elastic"
	"log"
)

func ElasticsearchTouchIndex(clientES *elastic.Client, index string) (err error) {
	log.Printf("确保索引存在: %s", index)
	var ok bool
	if ok, err = clientES.IndexExists(index).Do(context.Background()); err != nil {
		return
	}
	if !ok {
		if _, err = clientES.CreateIndex(index).Do(context.Background()); err != nil {
			return
		}
	}
	return
}

func ElasticsearchDisableRefresh(clientES *elastic.Client, index string) (err error) {
	log.Printf("调整索引刷新，为写入大量数据做准备: %s", index)
	_, err = clientES.IndexPutSettings(index).FlatSettings(true).BodyJson(map[string]interface{}{
		"index.refresh_interval": "2m",
	}).Do(context.Background())
	return
}

func ElasticsearchEnableRefresh(clientES *elastic.Client, index string) (err error) {
	log.Printf("恢复索引刷新: %s", index)
	_, err = clientES.IndexPutSettings(index).FlatSettings(true).BodyJson(map[string]interface{}{
		"index.refresh_interval": "10s",
	}).Do(context.Background())
	return
}
