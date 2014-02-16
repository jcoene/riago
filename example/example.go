package main

import (
	"github.com/jcoene/riago"
	"log"
)

func main() {
	// Create a client with up to 10 connections
	client := riago.NewClient("127.0.0.1:8087", 10)

	// Create a Riak KV Get request
	req := &riago.RpbGetReq{
		Bucket: []byte("people"),
		Key:    []byte("bob"),
	}

	// Issue the get request using the Get method
	resp, err := client.Get(req)
	if err != nil {
		log.Fatalf("an error occured: %s", err)
	}

	// See how many records (siblings) were returned
	log.Printf("bob has %d records", len(resp.GetContent()))
}
