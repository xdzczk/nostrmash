package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/xdzczk/nostrmash/internal/config"
)

func main() {
	outPath := flag.String("out", filepath.FromSlash("docs/configuration.md"), "output markdown file")
	check := flag.Bool("check", false, "verify the output file matches generated content")
	flag.Parse()

	content := []byte(config.GenerateConfigurationMarkdown())
	if *check {
		existing, err := os.ReadFile(*outPath)
		if err != nil {
			log.Fatalf("read config docs for check: %v", err)
		}
		if !bytes.Equal(existing, content) {
			log.Fatalf("%s is out of date; run: go run ./cmd/configdoc -out %s", *outPath, *outPath)
		}
		fmt.Printf("%s is up to date\n", *outPath)
		return
	}
	if err := os.WriteFile(*outPath, content, 0o644); err != nil {
		log.Fatalf("write config docs: %v", err)
	}
}
