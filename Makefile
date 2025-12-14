.PHONY: build run clean test

# Build the server
build:
	go build -o dashmq main.go

# Run the server
run: build
	./dashmq

# Run with custom port
run-port:
	go run main.go -port 9092

# Clean build artifacts
clean:
	rm -f dashmq dashmq.exe
	rm -rf data/

# Run tests
test:
	go test ./...

# Install dependencies
deps:
	go mod download
	go mod tidy


