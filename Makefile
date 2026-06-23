SHELL := /bin/bash

.PHONY: build test clean install run-example help

help:
	@echo "Pulsar Lite development commands"
	@echo ""
	@echo "  make build         - Build the Rust broker with rocksdb-storage"
	@echo "  make test          - Run Rust and Python tests"
	@echo "  make test-rust     - Run Rust tests"
	@echo "  make test-python   - Run Python integration tests"
	@echo "  make install       - Install the Python package in editable mode"
	@echo "  make run-example   - Run the example"
	@echo "  make clean         - Remove build artifacts"
	@echo "  make fmt           - Format Rust and Python code"
	@echo "  make lint          - Run Rust and Python lint checks"
	@echo ""

build:
	@echo "Building Rust broker..."
	cd rust && cargo build --release --features rocksdb-storage
	@echo "Build complete"

test: test-rust test-python

test-rust:
	@echo "Running Rust tests..."
	cd rust && cargo test --features rocksdb-storage

test-python:
	@echo "Running Python tests..."
	cd rust && cargo build --release --features rocksdb-storage
	scripts/run_python_tests_with_broker.sh

install:
	@echo "Installing Python SDK..."
	cd python && pip install -e ".[dev]"
	@echo "Installation complete"

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
	@echo "Clean complete"

fmt:
	@echo "Formatting code..."
	cd rust && cargo fmt
	cd python && ruff format . ../tests ../examples
	cd python && ruff check . ../tests ../examples --fix
	@echo "Format complete"

lint:
	@echo "Linting code..."
	scripts/run_rust_clippy.sh
	cd python && ruff check . ../tests ../examples
	@echo "Lint complete"

watch:
	@echo "Watching for changes..."
	cd rust && cargo watch -x "build --release --features rocksdb-storage"

.PHONY: all
all: build install test
