.PHONY: build test test-it up down acceptance fmt vet

build:
	go build ./...

test:            ## 单元测试（不依赖外部服务）
	go test -short ./...

test-it:         ## 集成测试（内嵌 PostgreSQL + miniredis，首次下载 PG 二进制）
	go test ./internal/itest/ -v

up:              ## 一键启动完整环境
	docker compose up --build -d

down:
	docker compose down

acceptance:      ## 对运行中的服务执行验收脚本
	./scripts/acceptance.sh

fmt:
	gofmt -w .

vet:
	go vet ./...
