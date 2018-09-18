package main

import (
	"os"

	"github.com/anzot/mysqlbeat/cmd"

	_ "github.com/anzot/mysqlbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
