package tasks

import (
	"context"
	"errors"
	"github.com/guoyk93/conc"
	"github.com/olivere/elastic"
	"github.com/tencentyun/cos-go-sdk-v5"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

const (
	keyProject = "project"
)

type IndexMigrateOptions struct {
	ESClient         *elastic.Client
	COSClient        *cos.Client
	Dir              string
	Index            string
	Bulk             int
	Concurrency      int
	CompressionLevel int
}

func (opts IndexMigrateOptions) Workspace() string {
	return filepath.Join(opts.Dir, opts.Index)
}

func IndexMigrate(opts IndexMigrateOptions) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		log.Printf("确保工作目录: %s", opts.Workspace())
		if err = os.MkdirAll(opts.Workspace(), 0755); err != nil {
			return
		}
		log.Printf("打开索引并等待索引恢复: %s", opts.Index)
		if _, err = opts.ESClient.OpenIndex(opts.Index).WaitForActiveShards("all").Do(ctx); err != nil {
			return
		}
		log.Printf("获取索引中包含的项目: %s", opts.Index)
		var projects []string
		if err = IndexCollectProjects(opts, &projects).Do(ctx); err != nil {
			return
		}
		log.Printf("索引包含以下项目: %s", strings.Join(projects, ", "))
		done, total := int64(0), int64(len(projects))
		tasks := make([]conc.Task, 0, len(projects))
		for _, _project := range projects {
			project := _project
			tasks = append(tasks, conc.TaskFunc(func(ctx context.Context) error {
				pOpts := ProjectMigrateOptions{
					IndexMigrateOptions: opts,
					Project:             project,
				}
				atomic.AddInt64(&done, 1)
				log.Printf("项目进度: %d/%d", done, total)
				return ProjectMigrate(pOpts).Do(ctx)
			}))
		}
		if err = conc.ParallelWithLimit(opts.Concurrency, tasks...).Do(ctx); err != nil {
			return
		}
		log.Printf("删除本地目录: %s", opts.Workspace())
		if err = os.RemoveAll(opts.Workspace()); err != nil {
			return
		}
		return
	})
}

func IndexCollectProjects(opts IndexMigrateOptions, out *[]string) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		var res *elastic.SearchResult
		if res, err = opts.ESClient.Search(opts.Index).Aggregation(
			keyProject,
			elastic.NewTermsAggregation().Field(keyProject).Size(99999),
		).Size(0).Do(ctx); err != nil {
			return
		}
		termAgg, ok := res.Aggregations.Terms(keyProject)
		if !ok {
			err = errors.New("无法找到聚合结果")
			return
		}
		if termAgg.SumOfOtherDocCount > 0 {
			err = errors.New("聚合结果无法包含所有可能的项目")
			return
		}
		projects := make([]string, 0, len(termAgg.Buckets))
		for _, bucket := range termAgg.Buckets {
			key, ok := bucket.Key.(string)
			if !ok {
				err = errors.New("聚合结果出现非字符串值")
				return
			}
			projects = append(projects, key)
		}
		*out = projects
		return
	})
}
