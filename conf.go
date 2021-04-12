package main

import (
	"errors"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"strings"
)

type Conf struct {
	PProf struct {
		Bind string `yaml:"bind"`
	} `yaml:"pprof"`
	Workspace     string `yaml:"workspace"`
	Elasticsearch struct {
		URLs []string `yaml:"urls"`
	} `yaml:"elasticsearch"`
	COS struct {
		URL       string `yaml:"url"`
		SecretID  string `yaml:"secret_id"`
		SecretKey string `yaml:"secret_key"`
	} `yaml:"cos"`
}

func checkFieldStr(str *string, name string) error {
	*str = strings.TrimSpace(*str)
	if *str == "" {
		return errors.New("缺少配置文件字段: " + name)
	}
	return nil
}

func LoadConf(file string) (conf Conf, err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(file); err != nil {
		return
	}
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		return
	}
	if err = checkFieldStr(&conf.Workspace, "workspace"); err != nil {
		return
	}
	if err = checkFieldStr(&conf.COS.URL, "cos.url"); err != nil {
		return
	}
	if err = checkFieldStr(&conf.COS.SecretID, "cos.secret_id"); err != nil {
		return
	}
	if err = checkFieldStr(&conf.COS.SecretKey, "cos.secret_key"); err != nil {
		return
	}
	if err = checkFieldStr(&conf.PProf.Bind, "pprof.bind"); err != nil {
		return
	}
	return
}
