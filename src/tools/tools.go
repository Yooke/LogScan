package tools

import (
	"os"
	"encoding/json"
)

func ParseList(file string) map[string]string {
	list := make(map[string]string)
	f, err := os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	if err := json.NewDecoder(f).Decode(&list); err != nil {
		panic(err)
	}
	return list
}
