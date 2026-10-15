#!/usr/bin/env bash
# Check a CLI without resolving other workspace members' sibling path dependencies.
set -euo pipefail

case "${1:-}" in
    kai-tool|ice-tool) check_package=$1 ;;
    *)
        echo "Usage: $0 {kai-tool|ice-tool} [cargo-command] [arguments...]" >&2
        exit 2
        ;;
esac
shift
check_action=${1:-test}
if [ "$#" -gt 0 ]; then shift; fi
check_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$check_repo/target"
check_workspace=$(mktemp -d "$check_repo/target/check-$check_package.XXXXXX")
trap 'rm -rf "$check_workspace"' EXIT

cp -R "$check_repo/$check_package" "$check_workspace/$check_package"
# Retain workspace dependencies, patches, and profiles; narrow only the member list.
awk -v package="$check_package" '
    /^members = \[/ { print "members = [\"" package "\"]"; skip=1; next }
    skip && /^\]/ { skip=0; next }
    !skip { print }
' "$check_repo/Cargo.toml" > "$check_workspace/Cargo.toml"
if [ -f "$check_repo/Cargo.lock" ]; then
    cp "$check_repo/Cargo.lock" "$check_workspace/Cargo.lock"
fi
# Preserve the source identity when building Ice from the isolated copy. Ordinary
# Cargo builds can supply the same metadata explicitly; absent metadata is unknown.
if [ "$check_package" = ice-tool ]; then
    unset ICE_BUILD_REVISION ICE_BUILD_DIRTY
    if check_root=$(git -C "$check_repo" rev-parse --show-toplevel 2>/dev/null) && [ "$check_root" = "$check_repo" ] && check_revision=$(git -C "$check_repo" rev-parse --verify HEAD 2>/dev/null); then
        export ICE_BUILD_REVISION="$check_revision"
        if check_changes=$(git -C "$check_repo" status --porcelain --untracked-files=normal -- ice-tool Cargo.toml Cargo.lock scripts/check-cli.sh 2>/dev/null); then
            if [ -n "$check_changes" ]; then export ICE_BUILD_DIRTY=true; else export ICE_BUILD_DIRTY=false; fi
        fi
    fi
fi
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$check_repo/target/${check_package%-tool}-standalone}"
cargo "$check_action" --manifest-path "$check_workspace/Cargo.toml" -p "$check_package" "$@"
