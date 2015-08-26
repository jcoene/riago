default: test

test:
	go test -cover -race -v

proto:
	protoc --go_out=. *.proto