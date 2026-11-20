#!/usr/bin/env bash
set -Eeuo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
deploy_script="$script_dir/deploy-plugin.sh"
test_root=$(mktemp -d)
commit=0123456789abcdef0123456789abcdef01234567
files=(styles.css sync_core.js sync_core_bg.wasm sync_core_simd.js sync_core_simd_bg.wasm manifest.json main.js)

cleanup() {
    rm -rf -- "$test_root"
}
trap cleanup EXIT

fail() {
    echo "not ok - $*" >&2
    exit 1
}

new_case() {
    local root
    root=$(mktemp -d "$test_root/case.XXXXXX")
    mkdir "$root/source" "$root/dest"
    for file in "${files[@]}"; do
        printf 'new %s\n' "$file" > "$root/source/$file"
        printf 'old %s\n' "$file" > "$root/dest/$file"
    done
    printf '%s\n' 'preserve me' > "$root/dest/data.json"
    printf '%s\n' "$root"
}

assert_contents() {
    local root=$1
    local prefix=$2
    for file in "${files[@]}"; do
        [[ $(cat "$root/dest/$file") == "$prefix $file" ]] \
            || fail "unexpected ${file} after deployment"
    done
    [[ $(cat "$root/dest/data.json") == 'preserve me' ]] \
        || fail "deployment changed plugin settings"
}

test_success() {
    local root
    root=$(new_case)
    bash "$deploy_script" "$root/source" "$root/dest" "$commit" >/dev/null
    assert_contents "$root" new
    echo "ok - plugin deployment installs one complete artifact set"
}

test_preflight_failure() {
    local root status
    root=$(new_case)
    rm "$root/source/sync_core_bg.wasm"
    set +e
    bash "$deploy_script" "$root/source" "$root/dest" "$commit" >/dev/null 2>&1
    status=$?
    set -e
    [[ $status -eq 2 ]] || fail "missing artifact did not fail preflight"
    assert_contents "$root" old
    echo "ok - plugin preflight failure leaves every live artifact unchanged"
}

test_activation_failure() {
    local root status real_mv
    root=$(new_case)
    mkdir "$root/bin"
    real_mv=$(command -v mv)
    cat > "$root/bin/mv" <<'FAKE_MV'
#!/usr/bin/env bash
set -Eeuo pipefail
target=${!#}
marker=${FAKE_MV_MARKER:?}
if [[ $target == */sync_core_bg.wasm && ! -e $marker ]]; then
    : > "$marker"
    exit 71
fi
exec "${FAKE_REAL_MV:?}" "$@"
FAKE_MV
    chmod +x "$root/bin/mv"

    set +e
    PATH="$root/bin:$PATH" FAKE_MV_MARKER="$root/mv-failed" FAKE_REAL_MV="$real_mv" \
        bash "$deploy_script" "$root/source" "$root/dest" "$commit" \
        >/dev/null 2> "$root/stderr"
    status=$?
    set -e
    [[ $status -eq 71 ]] || fail "activation failure status was not preserved"
    assert_contents "$root" old
    grep -Fq 'plugin deployment failed; restoring' "$root/stderr" \
        || fail "activation failure did not report rollback"
    echo "ok - plugin activation failure restores every previous artifact"
}

test_locked_destination() {
    local root
    root=$(new_case)
    if flock "$root/dest" bash "$deploy_script" "$root/source" "$root/dest" "$commit"; then
        fail "concurrent installation accepted"
    fi
    assert_contents "$root" old
    echo "ok - concurrent installation leaves the active destination alone"
}

test_interrupted_activation() {
    local root status
    root=$(new_case)
    rm "$root/dest/styles.css"
    mv "$root/dest/sync_core.js" "$root/old-js"
    ln -s "$root/old-js" "$root/dest/sync_core.js"
    mkdir "$root/bin"
    cat > "$root/bin/mv" <<'FAKE_MV'
#!/usr/bin/env bash
if [[ $1 == -fT && $3 == */new/sync_core_bg.wasm ]]; then
    kill -TERM "$PPID"
    exit 1
fi
exec "${REAL_MV:?}" "$@"
FAKE_MV
    chmod +x "$root/bin/mv"
    status=0
    REAL_MV=$(command -v mv) PATH="$root/bin:$PATH" bash "$deploy_script" "$root/source" "$root/dest" "$commit" || status=$?
    [[ $status -eq 143 && ! -e $root/dest/styles.css && -L $root/dest/sync_core.js ]] || fail "interrupted deployment lost the original layout"
    [[ $(cat "$root/dest/sync_core.js") == 'old sync_core.js' ]] || fail "old link content changed"
    echo "ok - TERM restores a symlink and removes a newly installed artifact"
}

test_incomplete_rollback() {
    local root status=0
    root=$(new_case)
    mkdir "$root/bin"
    cat > "$root/bin/mv" <<'FAKE_MV'
#!/usr/bin/env bash
case "$3" in
    */new/sync_core_bg.wasm) exit 71 ;;
    */old/styles.css) exit 72 ;;
esac
exec "${REAL_MV:?}" "$@"
FAKE_MV
    chmod +x "$root/bin/mv"
    REAL_MV=$(command -v mv) PATH="$root/bin:$PATH" bash "$deploy_script" "$root/source" "$root/dest" "$commit" 2>"$root/stderr" || status=$?
    [[ $status -eq 71 ]] || fail "incomplete rollback masked the original error"
    local backups=( "$root/dest"/.obsetync-deploy-*/old/styles.css )
    [[ ${#backups[@]} -eq 1 && $(cat "${backups[0]}") == 'old styles.css' ]] || fail "rollback discarded the unrestored backup"
    grep -Fq 'recovery files retained' "$root/stderr" || fail "missing recovery location"
    echo "ok - failed restoration retains the backup and reports its location"
}

test_success
test_preflight_failure
test_activation_failure
test_locked_destination
test_interrupted_activation
test_incomplete_rollback
