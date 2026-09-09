#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 5 ]]; then
    echo "usage: deploy-server.sh DEST IMAGE_TAR STAGED_COMPOSE VERSION COMMIT" >&2
    exit 2
fi

dest=$1
image_tar=$2
staged_compose=$3
version=$4
commit=$5

if [[ $dest != /* || ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ || ! $commit =~ ^[0-9a-f]{40}$ ]]; then
    echo "invalid release deployment arguments" >&2
    exit 2
fi
if [[ ! -d $dest || ! -f $image_tar || ! -f $staged_compose ]]; then
    echo "release deployment input is missing" >&2
    exit 2
fi

cd "$dest"
backup_root=${OBSETYNC_DEPLOY_BACKUP_ROOT:-/backup/obsetync-deploy}
lock_file=${OBSETYNC_DEPLOY_LOCK_FILE:-/run/lock/obsetync-kopia-backup.lock}
if [[ $backup_root != /* || $lock_file != /* ]]; then
    echo "deployment backup root and lock file must be absolute paths" >&2
    exit 2
fi
exec 9>"$lock_file"
flock 9

docker compose -f "$staged_compose" config --quiet
current_image=$(docker inspect --format '{{.Image}}' obsetync-server)
timestamp=$(date -u +%Y-%m-%dT%H-%M-%SZ)
backup_dir="${backup_root}/${timestamp}-${commit}"
rollback_tag="obsetync/server:rollback-${commit}"
release_tag="ghcr.io/savmech/obsetync-nix:${version}-${commit}"

install -d -m 0755 "$backup_dir"
install -m 0644 docker-compose.yml "$backup_dir/docker-compose.yml"
docker inspect obsetync-server > "$backup_dir/container-inspect.json"
docker image save --output "$backup_dir/server-image.tar" "$current_image"
docker image tag "$current_image" "$rollback_tag"

docker load --input "$image_tar"
docker image tag obsetync-server:nix "$release_tag"

install -m 0644 "$staged_compose" .docker-compose.release

rollback_armed=0
rollback() {
    local original_status=$?
    local rollback_status=0
    trap - ERR
    set +e
    if [[ $rollback_armed -eq 1 ]]; then
        echo "release deployment failed; restoring the previous compose and image" >&2
        install -m 0644 "$backup_dir/docker-compose.yml" .docker-compose.rollback \
            || rollback_status=1
        mv -f .docker-compose.rollback docker-compose.yml || rollback_status=1
        docker image tag "$rollback_tag" obsetync/server:local || rollback_status=1
        OBSETYNC_SERVER_IMAGE="$rollback_tag" \
            docker compose up -d --wait --wait-timeout 120 server || rollback_status=1
        curl -fsS http://127.0.0.1:27182/health >/dev/null || rollback_status=1
    fi
    if [[ $rollback_status -ne 0 ]]; then
        echo "automatic rollback failed; use ${backup_dir}" >&2
    fi
    exit "$original_status"
}
trap rollback ERR

rollback_armed=1
mv -f .docker-compose.release docker-compose.yml
OBSETYNC_SERVER_IMAGE="$release_tag" \
    docker compose up -d --wait --wait-timeout 120 server

expected_image=$(docker image inspect --format '{{.Id}}' "$release_tag")
running_image=$(docker inspect --format '{{.Image}}' obsetync-server)
if [[ $running_image != "$expected_image" ]]; then
    echo "running container does not use the release image" >&2
    false
fi
curl -fsS http://127.0.0.1:27182/health >/dev/null
docker image tag "$release_tag" obsetync/server:local

rollback_armed=0
trap - ERR
rm -f "$image_tar" "$staged_compose" || true
echo "deployed ${version} at ${commit}; rollback bundle: ${backup_dir}"
