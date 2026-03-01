.PHONY: build run dev clean fmt vet test

-include hosts.mk

# 二进制名称
BINARY := polyscan
# 构建输出目录
BUILD_DIR := build

# Go 环境
export GOPROXY ?= https://goproxy.cn,direct

## build: 编译所有二进制
build:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/polyscan
	go build -o $(BUILD_DIR)/backfill-profiles ./cmd/backfill-profiles
	go build -o $(BUILD_DIR)/migrate-mongo2sqlite ./cmd/migrate-mongo2sqlite

## build-linux: 交叉编译 Linux amd64 二进制 (用于部署到服务器)
build-linux:
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/$(BINARY) ./cmd/polyscan
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/backfill-profiles ./cmd/backfill-profiles
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/migrate-mongo2sqlite ./cmd/migrate-mongo2sqlite

## run: 运行 (服务器上直接运行预编译的二进制，本地先编译)
run:
	@if [ -f $(BUILD_DIR)/$(BINARY) ]; then \
		./$(BUILD_DIR)/$(BINARY) config.yaml; \
	else \
		$(MAKE) build && ./$(BUILD_DIR)/$(BINARY) config.yaml; \
	fi

## dev: 开发模式运行 (debug 日志)
dev: build
	LOG_LEVEL=debug ./$(BUILD_DIR)/$(BINARY) config.yaml

## clean: 清理构建产物
clean:
	rm -rf $(BUILD_DIR)

## fmt: 格式化代码
fmt:
	go fmt ./...

## vet: 静态检查
vet:
	go vet ./...

## test: 运行测试
test:
	go test -v ./...

## deps: 安装/更新依赖
deps:
	go mod tidy

## help: 显示帮助
help:
	@echo "Usage: make [target]"
	@echo ""
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'
