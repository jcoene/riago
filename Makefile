COVERPROFILE=/tmp/c.out

default: test

test:
	go test -cover

cover:
	go test -v -coverprofile=$(COVERPROFILE)
	go tool cover -html=$(COVERPROFILE)
	rm $(COVERPROFILE)
