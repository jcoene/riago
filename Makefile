default: test

test:
	go test -cover

proto:
	protoc --go_out=. *.proto