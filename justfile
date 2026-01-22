# Default recipe
default:
    @just --list

# Generate protobuf code
proto:
    buf generate

# Lint protobuf files
proto-lint:
    buf lint

# Build all Go packages
build:
    go build ./...

# Run all tests
test:
    go test ./...

# Run tests with verbose output
test-v:
    go test -v ./...

# Run tests with coverage
test-cover:
    go test -cover ./...

# Format Go code
fmt:
    go fmt ./...

# Run linter
lint:
    golangci-lint run

# Clean generated files
clean:
    rm -f proto/configpb/*.pb.go

# Regenerate proto and build
gen: proto build

# Install mise tools
tools:
    mise install
