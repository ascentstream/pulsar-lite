#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BROKER_BIN="${ROOT_DIR}/rust/target/release/pulsar-lite"
LOG_FILE="${PULSAR_LITE_LOG_FILE:-/tmp/pulsar-lite.log}"
TEST_STATE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pulsar-lite-test.XXXXXX")"
DB_PATH="${PULSAR_LITE_DB_PATH:-${TEST_STATE_DIR}/pulsar-lite.db}"
METADATA_PATH="${PULSAR_LITE_METADATA_FILE:-${DB_PATH%.db}.metadata.json}"

if [[ ! -x "${BROKER_BIN}" ]]; then
  echo "Broker binary not found at ${BROKER_BIN}" >&2
  echo "Run: cargo build --manifest-path rust/Cargo.toml --release --features rocksdb-storage" >&2
  exit 1
fi

rm -f "${LOG_FILE}"

RUST_LOG="${RUST_LOG:-info}" "${BROKER_BIN}" --db-path "${DB_PATH}" >"${LOG_FILE}" 2>&1 &
broker_pid=$!

cleanup() {
  if kill -0 "${broker_pid}" >/dev/null 2>&1; then
    kill "${broker_pid}" >/dev/null 2>&1 || true
    wait "${broker_pid}" >/dev/null 2>&1 || true
  fi
  rm -rf "${TEST_STATE_DIR}"
}
trap cleanup EXIT

sleep 1

if ! kill -0 "${broker_pid}" >/dev/null 2>&1; then
  echo "Broker failed to start. Recent log output:" >&2
  tail -120 "${LOG_FILE}" >&2 || true
  exit 1
fi

cd "${ROOT_DIR}/python"
PULSAR_LITE_BINARY="${BROKER_BIN}" \
  PULSAR_LITE_DB_PATH="${DB_PATH}" \
  PULSAR_LITE_METADATA_FILE="${METADATA_PATH}" \
  pytest ../tests/ -q "$@"
