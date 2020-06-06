package main

import (
	"compress/gzip"
	"context"
	"github.com/guoyk93/esndjson"
	"github.com/olivere/elastic"
	"log"
)

func ElasticsearchOpenIndex(clientES *elastic.Client, index string) (err error) {
	log.Printf("打开索引并等待索引恢复: %s", index)
	_, err = clientES.OpenIndex(index).WaitForActiveShards("all").Do(context.Background())
	return
}

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
	log.Printf("关闭索引刷新，为写入大量数据做准备: %s", index)
	_, err = clientES.IndexPutSettings(index).FlatSettings(true).BodyJson(map[string]interface{}{
		"index.refresh_interval": "-1",
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

func ElasticsearchDeleteIndex(clientES *elastic.Client, index string) (err error) {
	log.Printf("删除索引: %s", index)
	_, err = clientES.DeleteIndex(index).Do(context.Background())
	return
}

func ElasticsearchExportToWorkspace(url, dir, index string, bulk int) (err error) {
	log.Printf("导出索引到本地文件: %s", index)
	var e esndjson.Exporter
	if e, err = esndjson.NewExporter(esndjson.ExporterOptions{
		URL:           url,
		Sniff:         false,
		Dir:           dir,
		Index:         index,
		Bulk:          bulk,
		SectionKey:    "project",
		Concurrency:   3,
		CompressLevel: gzip.BestCompression,
		Logger:        log.Printf,
	}); err != nil {
		return
	}
	err = e.Run(context.Background())
	return
}
