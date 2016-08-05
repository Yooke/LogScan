package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"mongo"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"tools"
)

type LogEntry struct {
	Aid        string
	IP         string
	Date       time.Time
	Method     string
	Path       string
	Query      string
	Code       string
	HandleTime string
}

var (
	loop    = make(map[string]*os.File)
	channel = make(chan *LogEntry, 1024)
	state   map[string]int64
	list    map[string]string
)

func init() {
	// 解析list.json文件
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "ERROR: Required the config file.")
		os.Exit(1)
	}
	list = tools.ParseList(os.Args[1])
	// 解析状态文件
	f, err := os.Open("/tmp/.LogScan_state")
	if err == nil {
		if err := json.NewDecoder(f).Decode(&state); err != nil {
			f.Close()
			fmt.Fprintf(os.Stderr, "Decode state file error: %s\n", err.Error())
			os.Exit(1)
		}
		f.Close()
	} else if !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Open state file error: %s\n", err.Error())
		os.Exit(1)
	}
}

func main() {
	start()
	go handleSave()
	// 监听退出信号，在退出时记录当前的位置信息
	channel := make(chan os.Signal)
	signal.Notify(channel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for {
		sig := <-channel
		switch {
		case sig == syscall.SIGHUP:
			for _, f := range loop {
				f.Close()
			}
			state = make(map[string]int64)
			start()
		default:
			clean()
			os.Exit(0)
		}
	}
}

func start() {
	// 根据list.json文件，打开需要扫描的日志文件
	for k, v := range list {
		f, err := os.OpenFile(v, os.O_RDONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Open file error: %s\n", err.Error())
			clean()
			os.Exit(1)
		}
		// 如果存在上次扫描记录的位置，则直接Seek到上次的位置
		if offset, ok := state[k]; ok {
			f.Seek(offset, 0)
		}
		loop[k] = f
	}
	// 开启读GoRoutine
	for k := range list {
		go handleRead(k, loop[k])
	}
}

// 关闭并记录位置
func clean() {
	fmt.Println("Clean ...")
	state := make(map[string]int64)
	for k, f := range loop {
		offset, _ := f.Seek(0, os.SEEK_CUR)
		state[k] = offset
		f.Close()
	}
	f, err := os.Create(".state")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Open state file error: %s\n", err.Error())
		os.Exit(1)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(state)
}

// 读取日志
func handleRead(aid string, f *os.File) {
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		if err == nil {
			analyze(aid, line)
			continue
		} else if strings.Contains(err.Error(), "bad file descriptor") {
			return
		} else if err != io.EOF {
			fmt.Fprintf(os.Stderr, err.Error())
		}
		time.Sleep(1 * time.Second)
	}
}

// 分析日志
func analyze(aid, line string) {
	doc := LogEntry{Aid: aid}
	U := []string{}
	L := strings.Split(line, " ")
	if len(L) < 10 {
		return
	}
	if strings.Contains(L[4], "?") {
		U = strings.SplitN(L[4], "?", 2)
	} else {
		U = []string{L[4], ""}
	}
	doc.IP = L[0]
	doc.Method = L[3]
	doc.Path = U[0]
	doc.Query = U[1]
	doc.Code = L[6]
	doc.HandleTime = L[8]
	doc.Date, _ = time.ParseInLocation("2/Jan/2006:15:04:05", L[1], time.FixedZone("CST", 28800))
	channel <- &doc
}

// 处理写
func handleSave() {
	ticker := time.NewTicker(3 * time.Second)
	docs := []interface{}{}
	for {
		if len(docs) == 10 {
			mongo.SaveLog(docs...)
			docs = []interface{}{}
		}
		select {
		case doc := <-channel:
			docs = append(docs, doc)
		case <-ticker.C:
			if len(docs) > 0 {
				mongo.SaveLog(docs...)
				docs = []interface{}{}
			}
		}
	}
}
