package main

import (
	"os"
	"os/signal"
	"syscall"
	"modules/write"
	"fmt"
	"modules/analyze"
)

func main() {
	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "run":
		write.Start()
		channel := make(chan os.Signal)
		signal.Notify(channel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		for {
			sig := <-channel
			switch {
			case sig == syscall.SIGHUP:
				os.Setenv("_LOGSCAN_ROTATE", "true")
				write.Start()
			default:
				write.Stop()
				os.Exit(0)
			}
		}
	case "analyze":
		analyze.Analyze()
	default:
		printHelp()
	}
	os.Exit(0)
}

// 打印帮助信息
func printHelp() {
	fmt.Printf("Usage: %s COMMAND\n\n", os.Args[0])
	fmt.Println("Commands:")
	fmt.Println("	run	Start")
	fmt.Println("	analyze	Analyze")
}