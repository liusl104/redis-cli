# Go parameters
GOCMD=go
GOOS    := $(if $(GOOS),$(GOOS),$(shell go env GOOS))
GOARCH  := $(if $(GOARCH),$(GOARCH),$(shell go env GOARCH))
GOENV   := GO111MODULE=on CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH)
GO      := $(GOENV) go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) mod
BINARY_NAME=cmd/redis-cli
LINUX_BINARY_NAME=cmd/redis-cli_for_linux_x86_64
MAC_BINARY_NAME=cmd/redis-cli_for_mac_x86_64
WINDOWS_BINARY_NAME=cmd/redis-cli_for_windows_x86_64.exe
MAIN_NAME=redis-cli.go
PKG=redis-cli
GITHASH := $(shell git rev-parse --verify --short HEAD)
LDFLAGS += -X "$(PKG)/src.GitHash=$(GITHASH)"

all: build

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=$(GOARCH) $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(LINUX_BINARY_NAME)  $(MAIN_NAME)

windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=$(GOARCH) $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(WINDOWS_BINARY_NAME)  $(MAIN_NAME)

darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=$(GOARCH) $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(MAC_BINARY_NAME)  $(MAIN_NAME)

build:
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(BINARY_NAME)  $(MAIN_NAME)

mod:
	$(GOCMD) mod tidy
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME) $(LINUX_BINARY_NAME) $(MAC_BINARY_NAME)  $(WINDOWS_BINARY_NAME)
run:
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(BINARY_NAME)
	./$(BINARY_NAME)
