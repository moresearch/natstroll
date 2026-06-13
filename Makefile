# Natstroll Makefile

APP     := natstroll
HUB     := $(APP)-hub
SPOKE   := $(APP)-spoke

# Build info
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TS  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X main.Version=$(VERSION) \
	-X main.Commit=$(COMMIT) \
	-X main.BuildTime=$(BUILD_TS)

# Output directories
BIN_DIR := bin
DIST_DIR := dist

# Platforms for release builds
PLATFORMS := linux/amd64 linux/arm64 windows/amd64 windows/arm64

.DEFAULT_GOAL := help

# ─── help ─────────────────────────────────────────────────────────────────────

help: ## Show this help
	@echo "Natstroll — NATS capability test"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ─── build ────────────────────────────────────────────────────────────────────

.PHONY: all build
all: build ## Build everything (default)
build: hub spoke ## Build hub and spoke binaries

hub: ## Build hub binary for current platform
	@mkdir -p $(BIN_DIR)
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(HUB) ./cmd/hub

spoke: ## Build spoke binary for current platform
	@mkdir -p $(BIN_DIR)
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(SPOKE) ./cmd/spoke

# ─── test ─────────────────────────────────────────────────────────────────────

.PHONY: test test-verbose cover vet
test: ## Run all tests
	go test -count=1 ./...

test-verbose: ## Run all tests with verbose output
	go test -count=1 -v ./...

cover: ## Run tests with coverage report
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	@echo ""
	@echo "  HTML report: go tool cover -html=coverage.out"

vet: ## Run go vet on all packages
	go vet ./...

# ─── run ──────────────────────────────────────────────────────────────────────

.PHONY: run-hub run-spoke
run-hub: ## Run hub from source (needs NATS_ACCOUNT_SEED)
	go run ./cmd/hub

run-spoke: ## Run spoke from source (needs NATS_URL, REGISTRAR_CREDS_B64)
	go run ./cmd/spoke

# ─── release / cross-compile ──────────────────────────────────────────────────

.PHONY: release release-hub release-spoke

release: release-hub release-spoke ## Build all distributable binaries

release-hub: ## Build hub binaries for all target platforms
	@mkdir -p $(DIST_DIR)
	@for platform in $(PLATFORMS); do \
		GOOS=$${platform%/*} ; \
		GOARCH=$${platform#*/} ; \
		output="$(DIST_DIR)/$(HUB)-$${GOOS}-$${GOARCH}" ; \
		if [ "$${GOOS}" = "windows" ]; then output="$${output}.exe"; fi ; \
		echo "  $$output" ; \
		GOOS=$${GOOS} GOARCH=$${GOARCH} go build \
			-ldflags "$(LDFLAGS)" \
			-o "$$output" \
			./cmd/hub ; \
	done

release-spoke: ## Build spoke binaries for all target platforms
	@mkdir -p $(DIST_DIR)
	@for platform in $(PLATFORMS); do \
		GOOS=$${platform%/*} ; \
		GOARCH=$${platform#*/} ; \
		output="$(DIST_DIR)/$(SPOKE)-$${GOOS}-$${GOARCH}" ; \
		if [ "$${GOOS}" = "windows" ]; then output="$${output}.exe"; fi ; \
		echo "  $$output" ; \
		GOOS=$${GOOS} GOARCH=$${GOARCH} go build \
			-ldflags "$(LDFLAGS)" \
			-o "$$output" \
			./cmd/spoke ; \
	done

# ─── checksums ────────────────────────────────────────────────────────────────

.PHONY: checksums
checksums: release ## Generate SHA256 checksums for all release binaries
	@echo "SHA256 checksums:"
	@cd $(DIST_DIR) && sha256sum * 2>/dev/null | tee checksums.txt

# ─── clean ────────────────────────────────────────────────────────────────────

.PHONY: clean
clean: ## Remove build and distribution artifacts
	rm -rf $(BIN_DIR) $(DIST_DIR) coverage.out

# ─── tidy ─────────────────────────────────────────────────────────────────────

.PHONY: tidy fmt
tidy: ## Run go mod tidy
	go mod tidy

fmt: ## Run gofmt on all Go files
	gofmt -s -w .
