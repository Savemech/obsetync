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
    if [[ ! -s $source_dir/$file ]]; then
        echo "plugin artifact is missing or empty: ${file}" >&2
        exit 2
    fi
done

transaction_dir=$(mktemp -d "$dest_dir/.obsetync-deploy-${commit}.XXXXXX")
mkdir "$transaction_dir/new" "$transaction_dir/old" "$transaction_dir/missing"
for file in "${files[@]}"; do
    install -m 0644 "$source_dir/$file" "$transaction_dir/new/$file"
    if [[ -e $dest_dir/$file ]]; then
        cp -p "$dest_dir/$file" "$transaction_dir/old/$file"
    else
        : > "$transaction_dir/missing/$file"
    fi
done

rollback_armed=1
rollback() {
    local original_status=$?
    trap - ERR
    set +e
    if [[ $rollback_armed -eq 1 ]]; then
        echo "plugin deployment failed; restoring previous artifacts" >&2
        for file in "${files[@]}"; do
            if [[ -e $transaction_dir/missing/$file ]]; then
                rm -f "$dest_dir/$file"
            elif [[ -e $transaction_dir/old/$file ]]; then
                install -m 0644 "$transaction_dir/old/$file" "$dest_dir/.$file.rollback-$commit"
                mv -f "$dest_dir/.$file.rollback-$commit" "$dest_dir/$file"
            fi
            rm -f "$dest_dir/.$file.new-$commit" "$dest_dir/.$file.rollback-$commit"
        done
    fi
    rm -rf -- "$transaction_dir"
    exit "$original_status"
}
trap rollback ERR

for file in "${files[@]}"; do
    install -m 0644 "$transaction_dir/new/$file" "$dest_dir/.$file.new-$commit"
done
for file in "${files[@]}"; do
    mv -f "$dest_dir/.$file.new-$commit" "$dest_dir/$file"
done

rollback_armed=0
trap - ERR
rm -rf -- "$transaction_dir"
echo "deployed plugin artifacts at ${commit}"
