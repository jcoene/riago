# Riago

[![Build Status](https://secure.travis-ci.org/jcoene/riago.png?branch=master)](http://travis-ci.org/jcoene/riago)

Riago is a Riak client for Go. It's currently a work in progress.

## Why another client?

Riak is a nuanced data store with an explicit PBC (Protocol Buffers Client) API. Other Go clients make an effort to hide this API behind more friendly abstractions. This tends to make simple operations easy but can cause confusing behavior if your usage patterns don't match that of the client library.

Riago takes a different approach. Based on the assumption that the PBC API is the ideal development interface for Riak, Riago embraces and exposes these structures directly. Riago handles connection pooling, encoding and decoding, socket plumbing and error handling but otherwise gets out of your way.

## Features

- Protocol Buffers interface
- Connection pooling
- Customizable retry behavior
- Sane error handling (operation time errors, safe type assertions)

## Usage Example

```go
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
```

## TODO

- Support for more operations
- Instrumentation hooks

## License and Credits

Riago is licensed under the Apache license, see LICENSE.txt for details.

Riago is heavily inspired by and borrows from the [mrb/riakpbc](https://github.com/mrb/riakpbc) and [tpjg/goriakpbc](https://github.com/tpjg/goriakpbc) projects. Thank you to the original authors and all their contributors!
