package main

import (
	"context"
	"log"

	"example.com/__OLL_PLUGIN_NAME__/echo"
	plugin "github.com/onelastleaf/go-plugin-sdk"
)

func main() {
	runtime, err := plugin.New("__OLL_PLUGIN_ID__", "0.1.0").
		Action("echo", "Return the supplied arguments", echo.Run).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	if err := runtime.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
