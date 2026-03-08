#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo >/dev/null 2>&1; then
    echo "cargo is required but was not found in PATH" >&2
    exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

for path in \
    "$script_dir/arca-tool" \
    "$script_dir/ice-tool" \
    "$script_dir/kai-tool" \
    "$script_dir/ocular-tool"
do
    cargo install --path "$path"
done
