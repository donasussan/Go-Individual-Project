GO = go
BINARY = app  # Change this to your actual binary name
PACKAGES = $(shell go list ./...)

# Targets
.PHONY: all build clean test

all: build

build:
	@echo "Building $(BINARY)..."
	$(GO) build -o $(BINARY) main.go

clean:
	@echo "Cleaning up..."
	$(GO) clean
	rm -f $(BINARY)

test:
	@echo "Running tests..."
	$(GO) test $(PACKAGES)
run:
	@echo "Running $(BINARY)..."
	./$(BINARY)

