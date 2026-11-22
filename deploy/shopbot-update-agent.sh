#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="${SHOPBOT_PROJECT_DIR:-$HOME/remnawave-shopbot}"
DATA_DIR="${SHOPBOT_DATA_DIR:-$(dirname "$PROJECT_DIR")/remnawave-shopbot-data}"
UPDATE_DIR="$DATA_DIR/update"
REQUEST_FILE="$UPDATE_DIR/request"
STATUS_FILE="$UPDATE_DIR/status"
HEARTBEAT_FILE="$UPDATE_DIR/agent.seen"
LOG_FILE="$UPDATE_DIR/agent.log"
LOCK_FILE="$UPDATE_DIR/agent.lock"
HEALTH_URL="${SHOPBOT_HEALTH_URL:-http://127.0.0.1:1488/login}"
HEALTH_ATTEMPTS="${SHOPBOT_HEALTH_ATTEMPTS:-120}"
CONTAINER_NAME="${SHOPBOT_CONTAINER_NAME:-remnawave-shopbot}"
GIT_BRANCH="${SHOPBOT_GIT_BRANCH:-main}"

mkdir -p "$UPDATE_DIR" "$DATA_DIR/backups"
date +%s > "$HEARTBEAT_FILE"

exec 9>"$LOCK_FILE"
flock -n 9 || exit 0

log_line() {
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" >> "$LOG_FILE"
}

write_status() {
    printf 'state=%s\nmessage=%s\nupdated_at=%s\n' "$1" "$2" "$(date +%s)" > "$STATUS_FILE"
}

compose_cmd() {
    if docker compose version >/dev/null 2>&1; then
        docker compose "$@"
    else
        docker-compose "$@"
    fi
}

refresh_self() {
    local source="$PROJECT_DIR/deploy/shopbot-update-agent.sh"
    local target="/usr/local/bin/shopbot-update-agent"
    [ -f "$source" ] || return 0
    if cmp -s "$source" "$target"; then
        return 0
    fi
    cp -f "$source" "$target.new" 2>/dev/null || return 0
    chmod 0755 "$target.new" 2>/dev/null || return 0
    if mv -f "$target.new" "$target" 2>/dev/null; then
        log_line "agent script refreshed"
    fi
}

container_running() {
    [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null)" = "true" ]
}

container_present() {
    docker inspect "$CONTAINER_NAME" >/dev/null 2>&1
}

container_recover() {
    if container_running; then
        return 0
    fi
    if container_present; then
        log_line "контейнер не запущен, перезапускаю"
        docker restart "$CONTAINER_NAME" >> "$LOG_FILE" 2>&1 || docker start "$CONTAINER_NAME" >> "$LOG_FILE" 2>&1 || true
    else
        log_line "контейнер отсутствует, поднимаю через compose"
        compose_cmd up -d >> "$LOG_FILE" 2>&1 || true
    fi
}

container_log_tail() {
    docker logs --tail 40 "$CONTAINER_NAME" >> "$LOG_FILE" 2>&1 || true
}

force_recreate() {
    local previous_id="$1"
    [ -n "$previous_id" ] || return 0
    local current_id
    current_id="$(docker inspect -f '{{.Id}}' "$CONTAINER_NAME" 2>/dev/null || true)"
    if [ -n "$current_id" ] && [ "$current_id" = "$previous_id" ]; then
        log_line "контейнер не пересоздан, запускаю принудительное пересоздание"
        compose_cmd up -d --build --force-recreate >> "$LOG_FILE" 2>&1 || log_line "принудительное пересоздание вернуло ошибку"
    fi
}

wait_health() {
    local attempts="${1:-$HEALTH_ATTEMPTS}"
    local i=0
    while [ "$i" -lt "$attempts" ]; do
        if curl -fsS -o /dev/null --max-time 3 "$HEALTH_URL" 2>/dev/null; then
            return 0
        fi
        if [ $(((i + 1) % 15)) -eq 0 ]; then
            container_recover
        fi
        sleep 2
        i=$((i + 1))
    done
    log_line "панель не ответила, лог контейнера:"
    container_log_tail
    return 1
}

[ -f "$REQUEST_FILE" ] || exit 0

rm -f "$REQUEST_FILE"

if [ ! -d "$PROJECT_DIR/.git" ]; then
    write_status failed "Проект установлен не из git, автоматическое обновление недоступно"
    log_line "FAILED: no git repository"
    exit 0
fi

cd "$PROJECT_DIR"

PREVIOUS_COMMIT="$(git rev-parse HEAD)"
PREVIOUS_CONTAINER_ID="$(docker inspect -f '{{.Id}}' "$CONTAINER_NAME" 2>/dev/null || true)"
log_line "update started from $PREVIOUS_COMMIT"
write_status running "Идёт обновление"

if [ -f "$DATA_DIR/users.db" ]; then
    cp -f "$DATA_DIR/users.db" "$DATA_DIR/backups/pre-update-$(date +%Y%m%d-%H%M%S).db" 2>/dev/null || true
fi

if ! git fetch origin "$GIT_BRANCH" >> "$LOG_FILE" 2>&1; then
    write_status failed "Не удалось получить обновление из репозитория"
    log_line "FAILED: git fetch"
    exit 0
fi

if ! git reset --hard "origin/$GIT_BRANCH" >> "$LOG_FILE" 2>&1; then
    write_status failed "Не удалось применить обновление"
    log_line "FAILED: git reset"
    git reset --hard "$PREVIOUS_COMMIT" >> "$LOG_FILE" 2>&1 || true
    compose_cmd up -d --build >> "$LOG_FILE" 2>&1 || true
    exit 0
fi

compose_cmd up -d --build >> "$LOG_FILE" 2>&1 || log_line "compose вернул ошибку, проверяю панель"
force_recreate "$PREVIOUS_CONTAINER_ID"
container_recover

if wait_health "$HEALTH_ATTEMPTS"; then
    refresh_self
    write_status done "Обновление завершено"
    log_line "update finished"
    exit 0
fi

log_line "health check failed, rolling back"
write_status rolled_back "Панель не поднялась, выполняется откат"
git reset --hard "$PREVIOUS_COMMIT" >> "$LOG_FILE" 2>&1 || true
compose_cmd up -d --build --force-recreate >> "$LOG_FILE" 2>&1 || log_line "compose вернул ошибку при откате, проверяю панель"
container_recover
if wait_health "$HEALTH_ATTEMPTS"; then
    write_status rolled_back "Обновление отменено, предыдущая версия восстановлена"
    log_line "rollback finished"
else
    write_status failed "Откат не помог, проверьте контейнеры вручную"
    log_line "FAILED: rollback health check"
fi
exit 0
