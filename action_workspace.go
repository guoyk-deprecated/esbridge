package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/guoyk93/esbridge/esndjson"
	"github.com/guoyk93/progress"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func WorkspaceClear(dir string) (err error) {
	log.Printf("删除本地工作空间: %s", dir)
	if err = os.RemoveAll(dir); err != nil {
		return
	}
	return
}

func WorkspaceSetup(dir string) (err error) {
	log.Printf("建立本地工作空间: %s", dir)
	if err = os.RemoveAll(dir); err != nil {
		return
	}
	if err = os.MkdirAll(dir, 0755); err != nil {
		return
	}
	return
}

func WorkspaceUploadToCOS(dir string, clientCOS *cos.Client, index string) (err error) {
	log.Printf("导出索引到腾讯云存储: %s", index)

	var fis []os.FileInfo
	if fis, err = ioutil.ReadDir(dir); err != nil {
		return err
	}

	uploaded := 0

	p := progress.NewProgress(int64(len(fis)), fmt.Sprintf("导出索引到腾讯云存储: %s", index), log.Printf)
	for _, fi := range fis {
		p.Incr()
		if !strings.HasSuffix(fi.Name(), esndjson.ExtNDJSONGzipped) {
			err = fmt.Errorf("发现未知文件: %s", fi.Name())
			return
		}
		if _, _, err = clientCOS.Object.Upload(context.Background(), index+"/"+fi.Name(), filepath.Join(dir, fi.Name()), &cos.MultiUploadOptions{
			PartSize:       1000,
			ThreadPoolSize: runtime.NumCPU(),
			OptIni: &cos.InitiateMultipartUploadOptions{
				ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{XCosStorageClass: "STANDARD_IA"},
			},
		}); err != nil {
			return
		}

		uploaded++
		log.Printf("上传完成: %s", fi.Name())
	}

	if uploaded == 0 {
		err = errors.New("没有可上传的文件")
		return
	}
	return
}
