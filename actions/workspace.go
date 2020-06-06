package actions

import (
	"context"
	"fmt"
	"github.com/guoyk93/esbridge/pkg/exporter"
	"github.com/guoyk93/esbridge/pkg/progress"
	gzip "github.com/klauspost/pgzip"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
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

func WorkspaceGzipAll(dir string, level int) (err error) {
	log.Printf("压缩工作区: %s", dir)
	var fis []os.FileInfo
	if fis, err = ioutil.ReadDir(dir); err != nil {
		return err
	}

	p := progress.NewProgress(int64(len(fis)), fmt.Sprintf("压缩工作区: %s", dir))
	for _, fi := range fis {
		p.Incr()
		if !strings.HasSuffix(fi.Name(), exporter.Ext) {
			err = fmt.Errorf("发现未知文件: %s", fi.Name())
			return
		}
		filename := filepath.Join(dir, fi.Name())
		if err = WorkspaceGzipFile(filename, level); err != nil {
			return
		}
		if err = os.Remove(filepath.Join(dir, fi.Name())); err != nil {
			return
		}
		log.Printf("压缩完成: %s", fi.Name())
	}
	return
}

func WorkspaceGzipFile(filename string, level int) (err error) {
	var file *os.File
	if file, err = os.Open(filename); err != nil {
		return
	}
	defer file.Close()

	zname := strings.TrimSuffix(filename, exporter.Ext) + ExtGzipped
	var zfile *os.File
	if zfile, err = os.OpenFile(zname, os.O_RDWR|os.O_CREATE, 0640); err != nil {
		return
	}
	defer zfile.Close()

	var zw *gzip.Writer
	if zw, err = gzip.NewWriterLevel(zfile, level); err != nil {
		return
	}
	defer zw.Close()

	if _, err = io.Copy(zw, file); err != nil {
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

	p := progress.NewProgress(int64(len(fis)), fmt.Sprintf("导出索引到腾讯云存储: %s", index))
	for _, fi := range fis {
		p.Incr()
		if !strings.HasSuffix(fi.Name(), ExtGzipped) {
			err = fmt.Errorf("发现未知文件: %s", fi.Name())
			return
		}
		if _, _, err = clientCOS.Object.Upload(context.Background(), index+"/"+fi.Name(), filepath.Join(dir, fi.Name()), &cos.MultiUploadOptions{
			OptIni: &cos.InitiateMultipartUploadOptions{
				ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{XCosStorageClass: "STANDARD_IA"},
			},
		}); err != nil {
			return
		}
		log.Printf("上传完成: %s", fi.Name())
	}
	return
}
