# Riago

Riago is a Riak client for Go. It's currently a work in progress - not suitable for production use.

## Why another client?

Riak is a nuanced data store - proper usage is very important. It has an explicit API defined via protobuf. Other Go clients make an effort to hide the ugly protobuf interface behind more friendly abstractions. This tends to make simple tasks easy but can cause confusing behavior if your patterns don't match that of the client library.

Riago takes a different approach. Based on the assumption that the protobuf interface is the ideal developer API for Riak, Riago embraces and exposes the protobuf structures directly. Riago handles connection pooling, protobuf plumbing and error handling but otherwise gets out of your way.

## Features

- Protobuf interface
- Connection pooling
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
- Customizable retry behavior

## License and Credits

Riago is licensed under the Apache license, see LICENSE.txt for details. Riago is heavily inspired by and borrows from the mrb/riakpbc and tpjg/goriakpbc projects, thank you to the original authors and all their contributors.
