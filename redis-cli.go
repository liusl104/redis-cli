package main

import (
	"os"
	cli "redis-cli/src"
)

func main() {
	cli.ParseOptions(os.Args)
}
