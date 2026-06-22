# Contributing

Thank you for your interest in Pulsar Lite. This guide describes the expected
development workflow for local changes and pull requests.

## Requirements

- Rust stable with `rustfmt` and `clippy`.
- Python 3.10 or newer.
- `protobuf-compiler` / `protoc`.
- RocksDB build dependencies for the `rocksdb-storage` feature.

On Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y protobuf-compiler clang libclang-dev
```

On macOS:

```bash
brew install protobuf llvm
```

## Setup

```bash
git clone git@github.com:ascentstream/pulsar-lite.git
cd pulsar-lite

cd python
pip install -e ".[dev]"
pip install pre-commit
cd ..

pre-commit install
pre-commit install --hook-type commit-msg
pre-commit install --hook-type pre-push
```

Build the broker with persistent storage enabled:

```bash
cd rust
cargo build --release --features rocksdb-storage
```

## Development Workflow

Create a focused branch:

```bash
git switch main
git pull --ff-only
git switch -c feat/your-change
```

Run the checks before opening a pull request:

```bash
make fmt
make lint
make test
```

The Python integration tests require a broker binary with RocksDB support. The
`Makefile` handles that automatically. To run the test command manually:

```bash
cd rust
cargo build --release --features rocksdb-storage
cd ../python
PULSAR_LITE_BINARY=../rust/target/release/pulsar-lite pytest ../tests/ -q
```

## Commit Messages

Pulsar Lite uses Conventional Commits:

```text
type(optional-scope)!: description
```

Allowed types:

- `feat`
- `fix`
- `docs`
- `test`
- `refactor`
- `perf`
- `chore`
- `style`
- `ci`
- `build`
- `revert`

Examples:

```text
feat(broker): add persistent key-shared dispatch
fix(storage): preserve cursor state across restart
docs: rewrite repository README
ci: add pull request test workflow
```

The local `commit-msg` hook rejects vague titles such as `update stuff`.

## Code Style

Rust:

- Run `cargo fmt`.
- Run `cargo clippy --all-targets --features rocksdb-storage -- -D warnings`.
- Prefer small modules with explicit ownership boundaries.
- Keep protocol and storage behavior covered by tests.

Python:

- Run `ruff format`.
- Run `ruff check`.
- Keep public functions typed where practical.
- Use the official Pulsar client in integration tests when validating protocol behavior.

## Pull Requests

Every pull request should include:

- A concise summary.
- The behavior changed.
- The verification commands run locally.
- Any known limitations or follow-up work.

Pull requests must pass the GitHub Actions CI workflow before merge.

## Reporting Issues

Use GitHub Issues:

- Bugs: include reproduction steps, expected behavior, actual behavior, environment, and logs.
- Feature requests: include the use case, proposed behavior, and compatibility expectations.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0.
