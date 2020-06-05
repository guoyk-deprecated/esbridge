package actions

import (
	"context"
	"fmt"
	"github.com/guoyk93/esbridge/pkg/exporter"
	"github.com/guoyk93/esbridge/pkg/progress"
	"github.com/olivere/elastic"
	"io"
	"log"
)

func ESOpenIndex(clientES *elastic.Client, index string) (err error) {
	log.Printf("打开索引并等待索引恢复: %s", index)
	_, err = clientES.OpenIndex(index).WaitForActiveShards("all").Do(context.Background())
	return
}

func ESTouchIndex(clientES *elastic.Client, index string) (err error) {
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

func ESDisableRefresh(clientES *elastic.Client, index string) (err error) {
	log.Printf("关闭索引刷新，为写入大量数据做准备: %s", index)
	_, err = clientES.IndexPutSettings(index).FlatSettings(true).BodyJson(map[string]interface{}{
		"index.refresh_interval": "-1",
	}).Do(context.Background())
	return
}

func ESEnableRefresh(clientES *elastic.Client, index string) (err error) {
	log.Printf("恢复索引刷新: %s", index)
	_, err = clientES.IndexPutSettings(index).FlatSettings(true).BodyJson(map[string]interface{}{
		"index.refresh_interval": "10s",
	}).Do(context.Background())
	return
}

func ESDeleteIndex(clientES *elastic.Client, index string) (err error) {
	log.Printf("删除索引: %s", index)
	_, err = clientES.DeleteIndex(index).Do(context.Background())
	return
}

func ESExportToWorkspace(clientES *elastic.Client, dir, index string, level int) (err error) {
	log.Printf("导出索引到本地文件: %s", index)

	x := exporter.NewExporter(dir, level)
	defer x.Close()

	var total int64
	if total, err = clientES.Count(index).Do(context.Background()); err != nil {
		return
	}

	ss := clientES.Scroll(index).Type("_doc").Scroll("1m").Size(10000)
	defer ss.Clear(context.Background())

	p := progress.NewProgress(total, fmt.Sprintf("导出索引到本地文件: %s", index))

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
				if err = x.Append(*h.Source); err != nil {
					return
				}
			}
		}
	}
	return
}
