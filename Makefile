.PHONY: build test clean install run-example help

help:
	@echo "Pulsar Lite 开发命令"
	@echo ""
	@echo "  make build         - 构建 Rust 引擎"
	@echo "  make test          - 运行所有测试"
	@echo "  make test-rust     - 运行 Rust 测试"
	@echo "  make test-python   - 运行 Python 测试"
	@echo "  make install       - 安装 Python SDK（开发模式）"
	@echo "  make run-example   - 运行示例代码"
	@echo "  make clean         - 清理构建文件"
	@echo "  make fmt           - 格式化代码"
	@echo "  make lint          - 检查代码质量"
	@echo ""

build:
	@echo "Building Rust engine..."
	cd rust && cargo build --release
	@echo "✓ Build complete"

test: test-rust test-python

test-rust:
	@echo "Running Rust tests..."
	cd rust && cargo test

test-python:
	@echo "Running Python tests..."
	cd python && pytest ../tests/ -v

install:
	@echo "Installing Python SDK..."
	cd python && pip install -e ".[dev]"
	@echo "✓ Installation complete"

run-example:
	@echo "Running example..."
	cd python && python ../examples/basic_usage.py

clean:
	@echo "Cleaning..."
	rm -rf rust/target
	rm -rf python/build
	rm -rf python/dist
	rm -rf python/*.egg-info
	rm -rf __pycache__
	rm -rf tests/__pycache__
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -delete
	@echo "✓ Clean complete"

fmt:
	@echo "Formatting code..."
	cd rust && cargo fmt
	cd python && black . && ruff check . --fix || true
	@echo "✓ Format complete"

lint:
	@echo "Linting code..."
	cd rust && cargo clippy
	cd python && ruff check .
	@echo "✓ Lint complete"

watch:
	@echo "Watching for changes..."
	cd rust && cargo watch -x "build --release"

.PHONY: all
all: build install test
