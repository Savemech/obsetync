#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 3 ]]; then
    echo "usage: deploy-plugin.sh SOURCE_DIR DEST_DIR COMMIT" >&2
    exit 2
fi

source_dir=$1
dest_dir=$2
commit=$3
files=(
    styles.css
    sync_core.js
    sync_core_bg.wasm
    sync_core_simd.js
    sync_core_simd_bg.wasm
    manifest.json
    main.js
)

if [[ ! $commit =~ ^[0-9a-f]{40}$ || ! -d $source_dir || ! -d $dest_dir ]]; then
    echo "invalid plugin deployment arguments" >&2
    exit 2
fi

for file in "${files[@]}"; do
    if [[ ! -f $source_dir/$file || ! -s $source_dir/$file ]]; then
        echo "plugin artifact is missing or empty: ${file}" >&2
        exit 2
    fi
done

# Lock the directory inode so concurrent installs share the same lock even
# after a transaction directory has been removed.
exec {lock_fd}<"$dest_dir"
flock -n "$lock_fd" || { echo "plugin deployment already active or lock unavailable" >&2; exit 1; }

transaction_dir=$(mktemp -d "$dest_dir/.obsetync-deploy-${commit}.XXXXXX")
rollback_armed=0
finish() {
    local status=$? failed=0 file
    trap - EXIT
    trap '' HUP INT TERM
    if (( rollback_armed )); then
        echo "plugin deployment failed; restoring previous artifacts" >&2
        for file in "${files[@]}"; do
            if [[ -e $transaction_dir/old/$file || -L $transaction_dir/old/$file ]]; then
                mv -fT -- "$transaction_dir/old/$file" "$dest_dir/$file" || failed=1
            else
                rm -f -- "$dest_dir/$file" || failed=1
            fi
        done
    fi
    if (( failed )); then
        echo "rollback incomplete; recovery files retained at $transaction_dir" >&2
    else
        rm -rf -- "$transaction_dir"
    fi
    exit "$status"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir "$transaction_dir/new" "$transaction_dir/old"
for file in "${files[@]}"; do
    install -m 0644 -- "$source_dir/$file" "$transaction_dir/new/$file"
    if [[ -e $dest_dir/$file || -L $dest_dir/$file ]]; then
        [[ -f $dest_dir/$file || -L $dest_dir/$file ]] || { echo "expected regular plugin artifact: $file" >&2; exit 2; }
        cp -pP -- "$dest_dir/$file" "$transaction_dir/old/$file"
    fi
done

rollback_armed=1
for file in "${files[@]}"; do
    mv -fT -- "$transaction_dir/new/$file" "$dest_dir/$file"
done
rollback_armed=0
echo "deployed plugin artifacts at ${commit}"
