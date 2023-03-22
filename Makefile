# Go parameters
GOCMD=go
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
build:
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(BINARY_NAME)  $(MAIN_NAME)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(LINUX_BINARY_NAME)  $(MAIN_NAME)
other:
        CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(MAC_BINARY_NAME)  $(MAIN_NAME)
        CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(WINDOWS_BINARY_NAME)  $(MAIN_NAME)
mod:
	$(GOCMD) mod tidy
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME) $(LINUX_BINARY_NAME) $(MAC_BINARY_NAME)  $(WINDOWS_BINARY_NAME)
run:
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(BINARY_NAME)
	./$(BINARY_NAME)
