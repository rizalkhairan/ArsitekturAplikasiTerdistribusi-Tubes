package main

import (
	"log"
	"path/filepath"
)

func main() {
	path := env("DATA_PATH", filepath.Join("data", "events.json"))
	addr := env("ADDR", ":8080")
	store, err := newStore(path)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf(`{"level":"info","service":"canonical-store","msg":"started","path":%q,"addr":%q}`, path, addr)
	serve(store, addr)
}
