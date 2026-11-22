#!/usr/bin/env bash
set -Eeuo pipefail

BRAND_NAME="remnawave-shopbot"
PROJECT_NAME="remnawave-shopbot"
PROJECT_DIR="$HOME/$PROJECT_NAME"

if [[ ! -d "$PROJECT_DIR" ]] && [[ -d "./$PROJECT_NAME" ]]; then
    PROJECT_DIR="$(pwd)/$PROJECT_NAME"
fi

if [[ ! -d "$PROJECT_DIR" ]] && [[ -f "./pyproject.toml" ]] && [[ -d "./shop_bot" ]]; then
    PROJECT_DIR="$(pwd)"
fi

DATADIR="$(dirname "$PROJECT_DIR")/remnawave-shopbot-data"
NGINX_CONF="/etc/nginx/sites-available/${PROJECT_NAME}.conf"
NGINX_LINK="/etc/nginx/sites-enabled/${PROJECT_NAME}.conf"
AGENT_BIN="/usr/local/bin/shopbot-update-agent"
AGENT_ENV="/etc/default/shopbot-update"
AGENT_SERVICE="/etc/systemd/system/shopbot-update.service"
AGENT_TIMER="/etc/systemd/system/shopbot-update.timer"

DOMAIN=""
CHOICE=""
REMOVE_CONFIG=false
REMOVE_SSL=false
REMOVE_DOCKER=false
REMOVE_PROJECT=false
REMOVE_DATA=false
REMOVE_AGENT=false
REMOVE_CERTBOT=false

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
WHITE='\033[0;37m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

pause() {
    sleep "${1:-0.05}" 2>/dev/null || true
}

rule() {
    printf '%b  ────────────────────────────────────────────────────────────%b\n' "$DIM" "$NC"
}

print_banner() {
    local line="──────────────────────────────────────────────────────────"
    local pad_title pad_author
    pad_title="$(printf '%*s' $((58 - 3 - 17)) '')"
    pad_author="$(printf '%*s' $((58 - 3 - 13)) '')"
    clear 2>/dev/null || true
    printf '\n'
    printf '  %b╭%s╮%b\n' "$MAGENTA" "$line" "$NC"
    printf '  %b│%b%*s%b│%b\n' "$MAGENTA" "$NC" $((58)) '' "$MAGENTA" "$NC"
    printf '  %b│%b   %bREMNAWAVE SHOPBOT%b%s%b│%b\n' "$MAGENTA" "$NC" "$BOLD$WHITE" "$NC" "$pad_title" "$MAGENTA" "$NC"
    printf '  %b│%b   %bBy CyberERROR%b%s%b│%b\n' "$MAGENTA" "$NC" "$DIM" "$NC" "$pad_author" "$MAGENTA" "$NC"
    printf '  %b│%b%*s%b│%b\n' "$MAGENTA" "$NC" $((58)) '' "$MAGENTA" "$NC"
    printf '  %b╰%s╯%b\n' "$MAGENTA" "$line" "$NC"
    printf '\n'
    pause 0.08
    printf '  %bУДАЛЕНИЕ ПРОЕКТА%b\n' "$RED$BOLD" "$NC"
    printf '  %bВыберите, что нужно удалить — данные по умолчанию сохраняются%b\n' "$DIM" "$NC"
    pause 0.05
}

section() {
    printf '\n%b  ▸ %s%b  %b%s%b\n' "$CYAN" "$1" "$NC" "$BOLD" "$2" "$NC"
    rule
}

ok() {
    printf '%b  ✔%b %s\n' "$GREEN" "$NC" "$1"
}

warn() {
    printf '%b  ⚠%b %s\n' "$YELLOW" "$NC" "$1"
}

fail() {
    printf '%b  ✖%b %s\n' "$RED" "$NC" "$1" >&2
}

info() {
    printf '%b  ›%b %s\n' "$BLUE" "$NC" "$1"
}

kv() {
    printf '%b  %s%b  %s\n' "$DIM" "$1" "$NC" "$2"
}

spinner_run() {
    local message="$1"
    shift
    local -a cmd=("$@")
    local log_file
    log_file="$(mktemp)"
    local frames=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
    local frames_count=${#frames[@]}
    local started
    started="$(date +%s)"
    local index=0
    local interactive=0
    if [ -t 1 ]; then interactive=1; fi

    "${cmd[@]}" > "$log_file" 2>&1 &
    local pid=$!

    if [ "$interactive" -eq 1 ]; then
        tput civis 2>/dev/null || true
        while kill -0 "$pid" 2>/dev/null; do
            local frame="${frames[$((index % frames_count))]}"
            local elapsed=$(( $(date +%s) - started ))
            printf '\r\033[K  %b%s%b %s %b%sс%b' "$CYAN" "$frame" "$NC" "$message" "$DIM" "$elapsed" "$NC"
            index=$((index + 1))
            sleep 0.1
        done
    fi

    local code=0
    wait "$pid" || code=$?

    if [ "$interactive" -eq 1 ]; then
        tput cnorm 2>/dev/null || true
        printf '\r\033[K'
    fi

    local total=$(( $(date +%s) - started ))
    if [ "$code" -eq 0 ]; then
        printf '%b  ✔%b %s %b(%sс)%b\n' "$GREEN" "$NC" "$message" "$DIM" "$total" "$NC"
        rm -f "$log_file"
        return 0
    fi

    printf '%b  ✖%b %s\n' "$RED" "$NC" "$message"
    printf '%b  ─── вывод команды ───%b\n' "$RED" "$NC"
    tail -n 15 "$log_file" 2>/dev/null || true
    printf '%b  ─────────────────────%b\n' "$RED" "$NC"
    rm -f "$log_file"
    return "$code"
}

on_error() {
    tput cnorm 2>/dev/null || true
    printf '\n'
    fail "Скрипт прерван на строке $1."
    exit 1
}
trap 'on_error $LINENO' ERR

ask_yes() {
    local prompt="$1"
    local default_answer="$2"
    local reply=""
    printf '%b  ? %b%s ' "$CYAN" "$NC" "$prompt"
    read -r -n1 reply < /dev/tty || true
    printf '\n'
    if [[ -z "$reply" ]]; then
        reply="$default_answer"
    fi
    [[ "$reply" =~ [yYдД] ]]
}

ask_confirm_word() {
    local word="$1"
    local answer=""
    printf '%b  ? %bВведите %b%s%b для подтверждения: ' "$CYAN" "$NC" "$BOLD$RED" "$word" "$NC"
    read -r answer < /dev/tty || true
    printf '\n'
    [[ "$answer" == "$word" ]]
}

get_domain_from_nginx() {
    if [[ -f "$NGINX_CONF" ]]; then
        grep "server_name" "$NGINX_CONF" | awk '{print $2}' | sed 's/;//' | head -n1
    fi
}

safe_remove_dir() {
    local target="$1"
    if [[ -z "$target" ]]; then
        return 1
    fi
    if [[ "$target" == "/" ]] || [[ "$target" == "/root" ]] || [[ "$target" == "$HOME" ]]; then
        return 1
    fi
    if [[ ${#target} -lt 8 ]]; then
        return 1
    fi
    sudo rm -rf -- "$target"
}

compose_down() {
    if docker compose version >/dev/null 2>&1; then
        sudo bash -c "cd '$PROJECT_DIR' && docker compose down --rmi all --remove-orphans"
    else
        sudo bash -c "cd '$PROJECT_DIR' && docker-compose down --rmi all --remove-orphans"
    fi
}

project_present() {
    [[ -f "$NGINX_CONF" ]] || [[ -L "$NGINX_LINK" ]] || [[ -d "$PROJECT_DIR" ]] || [[ -d "$DATADIR" ]] || [[ -f "$AGENT_BIN" ]]
}

show_menu() {
    printf '\n  %bЧто удалить?%b\n\n' "$YELLOW$BOLD" "$NC"
    printf '   %b1%b  Конфигурация Nginx и SSL-сертификат    %b(быстро)%b\n' "$CYAN" "$NC" "$DIM" "$NC"
    printf '   %b2%b  Приложение целиком: контейнеры, код, агент\n' "$CYAN" "$NC"
    printf '   %b3%b  Максимальное: всё выше + Certbot\n' "$CYAN" "$NC"
    printf '   %b4%b  Настроить вручную\n' "$CYAN" "$NC"
    printf '   %b0%b  Отмена\n\n' "$CYAN" "$NC"
    printf '%b  ? %bВыберите вариант [0-4] (по умолчанию 0): ' "$CYAN" "$NC"
    read -r -n1 CHOICE < /dev/tty || true
    printf '\n'
    if [[ -z "$CHOICE" ]]; then
        CHOICE=0
    fi

    case "$CHOICE" in
        1)
            REMOVE_CONFIG=true
            REMOVE_SSL=true
            ;;
        2)
            REMOVE_CONFIG=true
            REMOVE_SSL=true
            REMOVE_DOCKER=true
            REMOVE_PROJECT=true
            REMOVE_AGENT=true
            ;;
        3)
            REMOVE_CONFIG=true
            REMOVE_SSL=true
            REMOVE_DOCKER=true
            REMOVE_PROJECT=true
            REMOVE_AGENT=true
            REMOVE_CERTBOT=true
            ;;
        4)
            show_custom_menu
            ;;
        0)
            warn "Удаление отменено."
            exit 0
            ;;
        *)
            warn "Неверный выбор. Отменено."
            exit 0
            ;;
    esac
}

show_custom_menu() {
    printf '\n  %bДополнительные опции%b\n\n' "$YELLOW$BOLD" "$NC"

    if [[ -f "$NGINX_CONF" ]] || [[ -L "$NGINX_LINK" ]]; then
        if ask_yes "Удалить конфигурацию Nginx? (Y/n):" "y"; then
            REMOVE_CONFIG=true
        fi
    fi

    DOMAIN=$(get_domain_from_nginx || true)
    if [[ -n "$DOMAIN" ]] && [[ -d "/etc/letsencrypt/live/${DOMAIN}" ]]; then
        if ask_yes "Удалить SSL-сертификат для ${DOMAIN}? (Y/n):" "y"; then
            REMOVE_SSL=true
        fi
    fi

    if command -v docker >/dev/null 2>&1 && docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q "$PROJECT_NAME"; then
        if ask_yes "Остановить и удалить контейнеры? (Y/n):" "y"; then
            REMOVE_DOCKER=true
        fi
    fi

    if [[ -f "$AGENT_TIMER" ]] || [[ -f "$AGENT_SERVICE" ]] || [[ -f "$AGENT_BIN" ]]; then
        if ask_yes "Удалить агент обновления (таймер, скрипт)? (Y/n):" "y"; then
            REMOVE_AGENT=true
        fi
    fi

    if [[ -d "$PROJECT_DIR" ]]; then
        if ask_yes "Удалить каталог проекта (${PROJECT_DIR})? (y/N):" "n"; then
            REMOVE_PROJECT=true
        fi
    fi

    if [[ -d "$DATADIR" ]]; then
        warn "В каталоге данных лежит база клиентов: ${DATADIR}"
        if ask_yes "Удалить каталог данных вместе с базой? (y/N):" "n"; then
            REMOVE_DATA=true
        fi
    fi

    if command -v certbot >/dev/null 2>&1; then
        if ask_yes "Удалить Certbot с сервера? (y/N):" "n"; then
            REMOVE_CERTBOT=true
        fi
    fi
}

show_summary() {
    printf '\n  %bИтоговый список%b\n\n' "$YELLOW$BOLD" "$NC"
    if [[ "$REMOVE_DOCKER" == "true" ]]; then kv "Контейнеры и образы" "удалить"; fi
    if [[ "$REMOVE_AGENT" == "true" ]]; then kv "Агент обновления" "удалить"; fi
    if [[ "$REMOVE_CONFIG" == "true" ]]; then kv "Конфигурация Nginx" "удалить"; fi
    if [[ "$REMOVE_SSL" == "true" ]]; then kv "SSL-сертификат" "удалить"; fi
    if [[ "$REMOVE_PROJECT" == "true" ]]; then kv "Каталог проекта" "$PROJECT_DIR"; fi
    if [[ "$REMOVE_DATA" == "true" ]]; then kv "Каталог данных" "$DATADIR"; else kv "Каталог данных" "сохранить"; fi
    if [[ "$REMOVE_CERTBOT" == "true" ]]; then kv "Certbot" "удалить"; fi
    printf '\n'

    if [[ "$CHOICE" == "1" ]]; then
        if ! ask_yes "Подтвердить удаление? (y/N):" "n"; then
            warn "Удаление отменено."
            exit 0
        fi
        return 0
    fi

    warn "Действие необратимо."
    if ! ask_confirm_word "УДАЛИТЬ"; then
        warn "Удаление отменено."
        exit 0
    fi
}

remove_update_agent() {
    if [[ "$REMOVE_AGENT" != "true" ]]; then
        return 0
    fi
    if systemctl list-unit-files 2>/dev/null | grep -q '^shopbot-update'; then
        spinner_run "Остановка таймера обновления" sudo systemctl disable --now shopbot-update.timer || warn "Не удалось остановить таймер"
    fi
    spinner_run "Удаление служб агента" sudo rm -f "$AGENT_SERVICE" "$AGENT_TIMER" || true
    spinner_run "Удаление настроек агента" sudo rm -f "$AGENT_ENV" || true
    spinner_run "Удаление скрипта агента" sudo rm -f "$AGENT_BIN" || true
    sudo systemctl daemon-reload 2>/dev/null || true
    ok "Агент обновления удалён"
}

remove_docker_containers() {
    if [[ "$REMOVE_DOCKER" != "true" ]]; then
        return 0
    fi
    if ! command -v docker >/dev/null 2>&1; then
        warn "Docker не установлен"
        return 0
    fi
    if [[ -f "$PROJECT_DIR/docker-compose.yml" ]]; then
        spinner_run "Остановка и удаление контейнеров" compose_down || warn "Не удалось удалить контейнеры через compose"
    fi
    local containers
    containers=$(docker ps -a --format '{{.Names}}' 2>/dev/null | grep "$PROJECT_NAME" || true)
    if [[ -n "$containers" ]]; then
        while read -r container; do
            spinner_run "Удаление контейнера ${container}" sudo docker rm -f "$container" || true
        done <<< "$containers"
    fi
    local images
    images=$(docker images --format '{{.Repository}}:{{.Tag}}' 2>/dev/null | grep "$PROJECT_NAME" || true)
    if [[ -n "$images" ]]; then
        while read -r image; do
            spinner_run "Удаление образа ${image}" sudo docker rmi -f "$image" || true
        done <<< "$images"
    fi
    ok "Docker-контейнеры и образы удалены"
}

remove_ssl_certs() {
    if [[ "$REMOVE_SSL" != "true" ]]; then
        return 0
    fi
    if [[ -z "$DOMAIN" ]]; then
        warn "Домен не определён, SSL-сертификат не удалён"
        return 0
    fi
    if [[ ! -d "/etc/letsencrypt/live/${DOMAIN}" ]] && [[ ! -f "/etc/letsencrypt/renewal/${DOMAIN}.conf" ]]; then
        warn "SSL-сертификат для ${DOMAIN} не найден"
        return 0
    fi
    spinner_run "Удаление сертификата ${DOMAIN}" sudo rm -rf "/etc/letsencrypt/live/${DOMAIN}" || true
    spinner_run "Удаление архива сертификата" sudo rm -rf "/etc/letsencrypt/archive/${DOMAIN}" || true
    spinner_run "Удаление конфигурации продления" sudo rm -f "/etc/letsencrypt/renewal/${DOMAIN}.conf" || true
    ok "SSL-сертификат удалён"
}

remove_nginx_config() {
    if [[ "$REMOVE_CONFIG" != "true" ]]; then
        return 0
    fi
    if [[ ! -f "$NGINX_CONF" ]] && [[ ! -L "$NGINX_LINK" ]]; then
        warn "Конфигурация Nginx не найдена"
        return 0
    fi
    spinner_run "Удаление ссылки Nginx" sudo rm -f "$NGINX_LINK" || true
    spinner_run "Удаление конфигурации Nginx" sudo rm -f "$NGINX_CONF" || true
    if command -v nginx >/dev/null 2>&1; then
        spinner_run "Проверка конфигурации Nginx" sudo nginx -t || warn "Проверка Nginx завершилась с ошибкой"
        spinner_run "Перезагрузка Nginx" sudo systemctl reload nginx || warn "Не удалось перезагрузить Nginx"
    fi
    ok "Конфигурация Nginx удалена"
}

remove_project_directory() {
    if [[ "$REMOVE_PROJECT" != "true" ]]; then
        return 0
    fi
    if [[ ! -d "$PROJECT_DIR" ]]; then
        warn "Каталог проекта не найден"
        return 0
    fi
    if [[ ! -f "$PROJECT_DIR/pyproject.toml" ]] && [[ ! -d "$PROJECT_DIR/shop_bot" ]]; then
        fail "Каталог ${PROJECT_DIR} не похож на проект, удаление отменено"
        return 0
    fi
    spinner_run "Удаление каталога проекта" safe_remove_dir "$PROJECT_DIR" || fail "Не удалось удалить ${PROJECT_DIR}"
    ok "Каталог проекта удалён"
}

remove_data_directory() {
    if [[ "$REMOVE_DATA" != "true" ]]; then
        return 0
    fi
    if [[ ! -d "$DATADIR" ]]; then
        warn "Каталог данных не найден"
        return 0
    fi
    spinner_run "Удаление каталога данных" safe_remove_dir "$DATADIR" || fail "Не удалось удалить ${DATADIR}"
    ok "Каталог данных удалён"
}

remove_certbot() {
    if [[ "$REMOVE_CERTBOT" != "true" ]]; then
        return 0
    fi
    if ! command -v certbot >/dev/null 2>&1; then
        warn "Certbot не установлен"
        return 0
    fi
    warn "Certbot может использоваться другими сайтами на этом сервере"
    if snap list 2>/dev/null | grep -q certbot; then
        spinner_run "Удаление Certbot (snap)" sudo snap remove certbot || true
    fi
    if dpkg -l 2>/dev/null | grep -q certbot; then
        spinner_run "Удаление Certbot (apt)" sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get remove -y python3-certbot-nginx certbot 2>/dev/null" || true
    fi
    ok "Certbot удалён"
}

show_leftovers() {
    local found=0
    if [[ -f "$NGINX_CONF" ]] || [[ -L "$NGINX_LINK" ]]; then
        warn "Осталась конфигурация Nginx: ${NGINX_CONF}"
        found=1
    fi
    if [[ -d "$PROJECT_DIR" ]]; then
        warn "Остался каталог проекта: ${PROJECT_DIR}"
        found=1
    fi
    if [[ -d "$DATADIR" ]]; then
        info "Сохранён каталог данных: ${DATADIR}"
    fi
    if [[ -f "$AGENT_BIN" ]] || [[ -f "$AGENT_TIMER" ]]; then
        warn "Остался агент обновления"
        found=1
    fi
    if command -v docker >/dev/null 2>&1 && docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q "$PROJECT_NAME"; then
        warn "Остались Docker-контейнеры проекта"
        found=1
    fi
    if [[ "$found" -eq 0 ]]; then
        ok "Лишних остатков не найдено"
    fi
}

final_card() {
    local line="──────────────────────────────────────────────────────────"
    local title="УДАЛЕНИЕ ЗАВЕРШЕНО"
    local pad
    pad="$(printf '%*s' $((58 - 4 - ${#title})) '')"
    printf '\n'
    printf '  %b╭%s╮%b\n' "$GREEN" "$line" "$NC"
    printf '  %b│%b ✔  %b%s%b%s%b│%b\n' "$GREEN" "$NC" "$BOLD$GREEN" "$title" "$NC" "$pad" "$GREEN" "$NC"
    printf '  %b╰%s╯%b\n' "$GREEN" "$line" "$NC"
    printf '\n'
    if [[ "$REMOVE_DATA" != "true" ]] && [[ -d "$DATADIR" ]]; then
        kv "Данные сохранены" "$DATADIR"
        kv "Вернуть позже" "запустите этот скрипт снова"
    fi
    kv "Репозиторий" "https://github.com/CyberERROR/remnawave-shopbot"
    printf '\n'
}

print_banner
info "Запрос прав администратора"
if ! sudo -v; then
    fail "Требуются права sudo"
    exit 1
fi
ok "Права подтверждены"

section "1/5" "Проверка установки"
if ! project_present; then
    warn "Установка ${BRAND_NAME} не найдена"
    exit 1
fi
ok "Найдена установка ${BRAND_NAME}"
kv "Каталог проекта" "$PROJECT_DIR"
kv "Каталог данных" "$DATADIR"
DOMAIN=$(get_domain_from_nginx || true)
if [[ -n "$DOMAIN" ]]; then
    kv "Домен" "$DOMAIN"
fi

section "2/5" "Что удаляем"
show_menu
show_summary

section "3/5" "Удаление"
printf '\n'
remove_docker_containers
remove_update_agent
remove_ssl_certs
remove_nginx_config
remove_project_directory
remove_data_directory
remove_certbot

section "4/5" "Проверка остатков"
show_leftovers

section "5/5" "Итог"
final_card
