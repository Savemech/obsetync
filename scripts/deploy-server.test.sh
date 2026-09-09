#!/usr/bin/env bash
set -Eeuo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
deploy_script="$script_dir/deploy-server.sh"
test_root=$(mktemp -d)

cleanup() {
    rm -rf -- "$test_root"
}
trap cleanup EXIT

fail() {
    echo "not ok - $*" >&2
    exit 1
}

assert_eq() {
    local expected=$1
    local actual=$2
    local message=$3
    [[ $actual == "$expected" ]] || fail "$message (expected '$expected', got '$actual')"
}

assert_file_contains() {
    local path=$1
    local expected=$2
    local message=$3
    grep -Fq -- "$expected" "$path" || fail "$message"
}

assert_file_eq() {
    local expected=$1
    local actual=$2
    local message=$3
    cmp -s -- "$expected" "$actual" || fail "$message"
}

new_case() {
    local root
    root=$(mktemp -d "$test_root/case.XXXXXX")
    mkdir -p "$root/bin" "$root/dest" "$root/backups" "$root/state"

    printf '%s\n' 'services: old' > "$root/dest/docker-compose.yml"
    printf '%s\n' 'services: release' > "$root/staged-compose.yml"
    printf '%s\n' 'release image' > "$root/release-image.tar"
    printf '%s\n' 'sha256:old' > "$root/state/running-image"
    : > "$root/docker.log"
    : > "$root/curl.log"

    cat > "$root/bin/docker" <<'FAKE_DOCKER'
#!/usr/bin/env bash
set -Eeuo pipefail

printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"

if [[ $1 == compose ]]; then
    if [[ ${2:-} == -f ]]; then
        [[ ${4:-} == config && ${5:-} == --quiet ]] || exit 91
        exit 0
    fi

    [[ ${2:-} == up ]] || exit 92
    printf 'compose up image=%s\n' "${OBSETYNC_SERVER_IMAGE:-unset}" >> "$FAKE_DOCKER_LOG"
    if [[ ${FAKE_DOCKER_DEPLOY_FAIL:-0} == 1 && $OBSETYNC_SERVER_IMAGE != *rollback-* ]]; then
        exit 1
    fi
    if [[ $OBSETYNC_SERVER_IMAGE == *rollback-* ]]; then
        printf '%s\n' 'sha256:old' > "$FAKE_DOCKER_STATE/running-image"
    else
        printf '%s\n' 'sha256:new' > "$FAKE_DOCKER_STATE/running-image"
    fi
    exit 0
fi

if [[ $1 == inspect ]]; then
    if [[ ${2:-} == --format ]]; then
        cat "$FAKE_DOCKER_STATE/running-image"
    else
        printf '%s\n' '[{"Image":"sha256:old"}]'
    fi
    exit 0
fi

if [[ $1 == load ]]; then
    [[ ${2:-} == --input && -f ${3:-} ]] || exit 93
    exit 0
fi

if [[ $1 == image ]]; then
    case ${2:-} in
        save)
            [[ ${3:-} == --output && -n ${4:-} && ${5:-} == sha256:old ]] || exit 94
            printf '%s\n' 'saved sha256:old' > "$4"
            ;;
        tag)
            [[ -n ${3:-} && -n ${4:-} ]] || exit 95
            ;;
        inspect)
            [[ ${3:-} == --format && -n ${4:-} && -n ${5:-} ]] || exit 96
            [[ ${FAKE_DOCKER_INSPECT_FAIL:-0} != 1 ]] || exit 99
            printf '%s\n' 'sha256:new'
            ;;
        *) exit 97 ;;
    esac
    exit 0
fi

exit 98
FAKE_DOCKER

    cat > "$root/bin/curl" <<'FAKE_CURL'
#!/usr/bin/env bash
set -Eeuo pipefail

printf '%s\n' "$*" >> "$FAKE_CURL_LOG"
if [[ ${FAKE_CURL_FAIL_FIRST:-0} == 1 && ! -e $FAKE_DOCKER_STATE/curl-failed ]]; then
    : > "$FAKE_DOCKER_STATE/curl-failed"
    exit 22
fi
exit 0
FAKE_CURL
    chmod +x "$root/bin/docker" "$root/bin/curl"
    printf '%s\n' "$root"
}

run_deploy() {
    local root=$1
    local deploy_fail=$2
    local health_fail=$3
    local inspect_fail=${4:-0}
    local version=1.12.0
    local commit=0123456789abcdef0123456789abcdef01234567

    set +e
    PATH="$root/bin:$PATH" \
        FAKE_DOCKER_LOG="$root/docker.log" \
        FAKE_CURL_LOG="$root/curl.log" \
        FAKE_DOCKER_STATE="$root/state" \
        FAKE_DOCKER_DEPLOY_FAIL="$deploy_fail" \
        FAKE_CURL_FAIL_FIRST="$health_fail" \
        FAKE_DOCKER_INSPECT_FAIL="$inspect_fail" \
        OBSETYNC_DEPLOY_BACKUP_ROOT="$root/backups" \
        OBSETYNC_DEPLOY_LOCK_FILE="$root/deploy.lock" \
        bash "$deploy_script" "$root/dest" "$root/release-image.tar" \
            "$root/staged-compose.yml" "$version" "$commit" \
            > "$root/stdout" 2> "$root/stderr"
    local status=$?
    set -e
    printf '%s\n' "$status"
}

find_backup() {
    local root=$1
    local commit=0123456789abcdef0123456789abcdef01234567
    local backups=("$root/backups"/*-"$commit")
    [[ ${#backups[@]} -eq 1 && -d ${backups[0]} ]] || fail "expected one rollback bundle"
    printf '%s\n' "${backups[0]}"
}

test_success() {
    local root status backup
    root=$(new_case)
    status=$(run_deploy "$root" 0 0)
    assert_eq 0 "$status" "successful deployment status"
    backup=$(find_backup "$root")

    assert_file_contains "$root/dest/docker-compose.yml" 'services: release' \
        "successful deployment did not install staged compose"
    assert_file_contains "$backup/docker-compose.yml" 'services: old' \
        "successful deployment did not preserve old compose"
    assert_file_contains "$backup/server-image.tar" 'saved sha256:old' \
        "successful deployment did not preserve old image"
    [[ ! -e $root/release-image.tar ]] || fail "successful deployment kept transferred image"
    [[ ! -e $root/staged-compose.yml ]] || fail "successful deployment kept staged compose"
    assert_eq sha256:new "$(cat "$root/state/running-image")" \
        "successful deployment did not leave release image running"
    assert_file_contains "$root/docker.log" \
        'compose up image=ghcr.io/savmech/obsetync-nix:1.12.0-0123456789abcdef0123456789abcdef01234567' \
        "successful deployment did not select immutable release tag"
    assert_eq 1 "$(wc -l < "$root/curl.log")" "successful deployment health check count"
    echo "ok - successful deployment preserves rollback inputs and installs the release"
}

test_deploy_failure_rollback() {
    local root status backup
    root=$(new_case)
    cp "$root/dest/docker-compose.yml" "$root/expected-old-compose.yml"
    status=$(run_deploy "$root" 1 0)
    assert_eq 1 "$status" "failed compose-up deployment status"
    backup=$(find_backup "$root")

    assert_file_eq "$root/expected-old-compose.yml" "$root/dest/docker-compose.yml" \
        "compose-up failure did not restore old compose"
    assert_file_contains "$backup/server-image.tar" 'saved sha256:old' \
        "compose-up failure did not preserve old image"
    assert_eq sha256:old "$(cat "$root/state/running-image")" \
        "compose-up failure did not leave rollback image running"
    assert_file_contains "$root/docker.log" \
        'compose up image=obsetync/server:rollback-0123456789abcdef0123456789abcdef01234567' \
        "compose-up failure did not start rollback image"
    assert_eq 1 "$(wc -l < "$root/curl.log")" "compose-up rollback health check count"
    assert_file_contains "$root/stderr" 'release deployment failed; restoring' \
        "compose-up failure did not report rollback"
    echo "ok - compose-up failure restores the previous compose and image"
}

test_health_failure_rollback() {
    local root status
    root=$(new_case)
    cp "$root/dest/docker-compose.yml" "$root/expected-old-compose.yml"
    status=$(run_deploy "$root" 0 1)
    assert_eq 22 "$status" "failed health-check deployment status"
    find_backup "$root" >/dev/null

    assert_file_eq "$root/expected-old-compose.yml" "$root/dest/docker-compose.yml" \
        "health-check failure did not restore old compose"
    assert_eq sha256:old "$(cat "$root/state/running-image")" \
        "health-check failure did not leave rollback image running"
    assert_file_contains "$root/docker.log" \
        'compose up image=obsetync/server:rollback-0123456789abcdef0123456789abcdef01234567' \
        "health-check failure did not start rollback image"
    assert_eq 2 "$(wc -l < "$root/curl.log")" "health failure and rollback health check count"
    assert_file_contains "$root/stderr" 'release deployment failed; restoring' \
        "health-check failure did not report rollback"
    echo "ok - health-check failure restores the previous compose and image"
}

test_post_mutation_command_failure_rollback() {
    local root status
    root=$(new_case)
    cp "$root/dest/docker-compose.yml" "$root/expected-old-compose.yml"
    status=$(run_deploy "$root" 0 0 1)
    assert_eq 99 "$status" "failed image-inspect deployment status"
    find_backup "$root" >/dev/null

    assert_file_eq "$root/expected-old-compose.yml" "$root/dest/docker-compose.yml" \
        "image-inspect failure did not restore old compose"
    assert_eq sha256:old "$(cat "$root/state/running-image")" \
        "image-inspect failure did not leave rollback image running"
    assert_file_contains "$root/docker.log" \
        'compose up image=obsetync/server:rollback-0123456789abcdef0123456789abcdef01234567' \
        "image-inspect failure did not start rollback image"
    assert_file_contains "$root/stderr" 'release deployment failed; restoring' \
        "image-inspect failure did not report rollback"
    echo "ok - post-mutation command failure restores the previous compose and image"
}

test_success
test_deploy_failure_rollback
test_health_failure_rollback
test_post_mutation_command_failure_rollback
