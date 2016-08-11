package write

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"mongo"
	"os"
	"strings"
	"time"
	"tools"
	"os/exec"
	"path"
	"config"
	"strconv"
)

type LogEntry struct {
	Aid        string
	IP         string
	Date       time.Time
	Method     string
	Path       string
	Query      string
	Code       string
	HandleTime float64
}

var (
	loop = make(map[string]*os.File)
	channel   = make(chan *LogEntry, 1024)
	state     map[string]int64
	list      map[string]string
)

func loadState() {
	// 解析list.json文件
	file, _ := exec.LookPath(os.Args[0])
	config.WorkDir = path.Dir(file)
	list = tools.ParseList(path.Join(config.WorkDir, "list.json"))
	// 解析状态文件
	f, err := os.Open(path.Join(config.WorkDir, ".LogScan_state"))
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

func Start() {
	// 如果为日志切割，则先关闭已打开的文件
	if os.Getenv("_LOGSCAN_ROTATE") == "true" {
		os.Unsetenv("_LOGSCAN_ROTATE")
		state = make(map[string]int64)
		for _, f := range loop {
			f.Close()
		}
	} else {
		loadState()
		go handleSave()
	}
	// 根据list.json文件，打开需要扫描的日志文件
	for k, v := range list {
		f, err := os.OpenFile(v, os.O_RDONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Open file error: %s\n", err.Error())
			Stop()
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
func Stop() {
	fmt.Println("Clean ...")
	state := make(map[string]int64)
	for k, f := range loop {
		offset, _ := f.Seek(0, os.SEEK_CUR)
		state[k] = offset
		f.Close()
	}
	f, err := os.Create(path.Join(config.WorkDir, ".LogScan_state"))
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
	doc.HandleTime, _ = strconv.ParseFloat(L[8], 32)
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
