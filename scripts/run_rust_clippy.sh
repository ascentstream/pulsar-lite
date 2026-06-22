#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT_DIR}/rust"
cargo clippy --all-targets --features rocksdb-storage -- \
  -D warnings \
  -A clippy::too_many_arguments \
  -A clippy::module_inception \
  -A clippy::inherent_to_string \
  -A clippy::empty_docs
