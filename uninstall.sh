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

UI_KIND="uninstall"
# Shared terminal UI, embedded so each script remains standalone.
UI_KIND="${UI_KIND:-install}"
UI_TTY=0 UI_ANIMATE=1 UI_COLOR=1 UI_DEMO=0 UI_WARNINGS=0 UI_ERRORS=0
UI_PID="" UI_LOG="" UI_STARTED=$SECONDS UI_COLS=80
RED='' GREEN='' YELLOW='' BLUE='' CYAN='' MAGENTA='' WHITE='' BOLD='' DIM='' NC=''
ACCENT='' RAIL='' MARK_OK='✓' MARK_FAIL='×' MARK_WARN='!' BAR_ON='━' BAR_OFF='─'

ui_width() {
    UI_COLS="$(tput cols 2>/dev/null || printf '80')"
    [[ "$UI_COLS" =~ ^[0-9]+$ ]] || UI_COLS=80
    (( UI_COLS > 88 )) && UI_COLS=88
    (( UI_COLS < 20 )) && UI_COLS=20
    return 0
}

ui_init() {
    local arg
    [[ -t 1 && "${TERM:-dumb}" != dumb ]] && UI_TTY=1
    for arg in "$@"; do
        case "$arg" in
            --demo) UI_DEMO=1 ;;
            --no-animation) UI_ANIMATE=0 ;;
            --no-color) UI_COLOR=0 ;;
            -h|--help)
                printf '\n  REMNAWAVE SHOPBOT · CyberERROR\n\n'
                printf '  bash %s [--demo] [--no-animation] [--no-color]\n\n' "${0##*/}"
                printf '  --demo           Только показ интерфейса, без изменений системы\n'
                printf '  --no-animation   Статичный вывод без анимации\n'
                printf '  --no-color       Вывод без цветовых кодов\n\n'
                exit 0 ;;
            *) printf 'Неизвестный параметр: %s\n' "$arg" >&2; exit 2 ;;
        esac
    done
    [[ ${NO_COLOR+x} ]] && UI_COLOR=0
    if (( ! UI_TTY )); then UI_ANIMATE=0; UI_COLOR=0; fi
    if (( UI_COLOR )); then
        RED='\033[91m' GREEN='\033[92m' YELLOW='\033[93m'
        BLUE='\033[96m' CYAN='\033[96m' MAGENTA='\033[97m'
        WHITE='\033[97m' BOLD='\033[1m' DIM='\033[90m' NC='\033[0m'
        if [[ "${COLORTERM:-}" == truecolor || "${TERM:-}" == *256color* ]]; then
            RED='\033[38;5;203m' GREEN='\033[38;5;121m' YELLOW='\033[38;5;222m'
            CYAN='\033[38;5;87m' BLUE="$CYAN" DIM='\033[38;5;245m'
        fi
    fi
    ACCENT="$CYAN"; [[ "$UI_KIND" == uninstall ]] && ACCENT="$YELLOW"
    RAIL="$DIM"
    case "${LC_ALL:-${LC_CTYPE:-${LANG:-}}}" in
        *[Uu][Tt][Ff]*8*) ;;
        *) MARK_OK='+'; MARK_FAIL='x'; BAR_ON='='; BAR_OFF='-' ;;
    esac
    ui_width
    trap 'ui_cleanup' EXIT
    trap 'ui_interrupt 130' INT
    trap 'ui_interrupt 143' TERM
    trap 'on_error "$LINENO" "$?"' ERR
}

ui_cleanup() {
    # Restore the cursor even when a command fails or the user cancels.
    if (( UI_TTY && UI_ANIMATE )); then printf '\033[?25h'; fi
    if [[ -n "$UI_LOG" && -f "$UI_LOG" ]]; then rm -f -- "$UI_LOG"; fi
    return 0
}

ui_interrupt() {
    trap '' INT TERM
    if [[ -n "$UI_PID" ]]; then
        kill -TERM "$UI_PID" 2>/dev/null || true
        wait "$UI_PID" 2>/dev/null || true
        UI_PID=''
    fi
    (( UI_TTY && UI_ANIMATE )) && printf '\r\033[2K'
    printf '\n'
    warn 'Операция прервана. Уже выполненные действия не отменяются.'
    exit "$1"
}

on_error() {
    local line="$1" code="${2:-1}"
    (( UI_TTY && UI_ANIMATE )) && printf '\r\033[2K'
    printf '\n'
    fail "Операция остановлена · строка ${line} · код ${code}"
    info 'Проверьте сообщение об ошибке выше перед повторным запуском.'
    exit "$code"
}

pause() {
    if (( UI_ANIMATE )); then sleep "${1:-0.04}"; fi
    return 0
}

rule() {
    local i
    printf '  %b' "$RAIL"
    for ((i=0; i<UI_COLS-4; i++)); do printf '%s' "$BAR_OFF"; done
    printf '%b\n' "$NC"
}

ui_bar() {
    local done="$1" total="$2" width=$((UI_COLS-26)) i filled
    (( width > 32 )) && width=32
    (( width < 8 )) && width=8
    filled=$((done * width / total))
    printf '  %b' "$ACCENT"
    for ((i=0; i<width; i++)); do
        if (( i < filled )); then printf '%s' "$BAR_ON";
        else printf '%b%s' "$DIM" "$BAR_OFF"; fi
    done
    printf '%b  %b%02d / %02d этапов%b\n' "$NC" "$DIM" "$done" "$total" "$NC"
}

print_banner() {
    local label="${1:-УДАЛЕНИЕ ПРОЕКТА}" version="${2:-}"
    printf '\n'
    rule
    printf '\n  %bR E M N A W A V E%b\n' "$BOLD$WHITE" "$NC"
    printf '  %bSHOPBOT%b  %b/  CyberERROR%b\n\n' "$BOLD$ACCENT" "$NC" "$DIM" "$NC"
    printf '  %b●  %s%b\n' "$ACCENT" "$label" "$NC"
    [[ -z "$version" ]] || printf '  %bВерсия %s%b\n' "$DIM" "$version" "$NC"
    if [[ "$UI_KIND" == uninstall ]]; then
        printf '  %bВыбор компонентов → подтверждение → удаление%b\n' "$DIM" "$NC"
    else
        printf '  %bОкружение → настройка → запуск%b\n' "$DIM" "$NC"
    fi
    if (( UI_DEMO )); then printf '  %bДЕМО · команды установки и удаления не выполняются%b\n' "$YELLOW" "$NC"; fi
    printf '\n'
    rule
    pause 0.12
}

section() {
    local step="${1%/*}" total="${1#*/}"
    ui_width
    printf '\n'
    ui_bar "$((step-1))" "$total"
    printf '\n  %b%s%b  %b%s%b\n\n' "$BOLD$ACCENT" "$1" "$NC" "$BOLD$WHITE" "$2" "$NC"
}

ok() { printf '  %b%s%b  %s\n' "$GREEN" "$MARK_OK" "$NC" "$1"; }
warn() { UI_WARNINGS=$((UI_WARNINGS+1)); printf '  %b%s%b  %s\n' "$YELLOW" "$MARK_WARN" "$NC" "$1"; }
fail() { UI_ERRORS=$((UI_ERRORS+1)); printf '  %b%s%b  %s\n' "$RED" "$MARK_FAIL" "$NC" "$1" >&2; }
info() { printf '  %b›%b  %s\n' "$ACCENT" "$NC" "$1"; }
kv() {
    printf '  %b%s%b\n    %s\n' "$DIM" "$1" "$NC" "$2"
}

ui_read() {
    # Read from the terminal, including when the script arrives through curl | bash.
    local target="$1" value=''
    if ! { IFS= read -r value </dev/tty; } 2>/dev/null; then
        fail 'Нет ввода из терминала. Запустите скрипт в интерактивной SSH-сессии.'
        exit 1
    fi
    printf -v "$target" '%s' "$value"
}

spinner_run() {
    local message="$1"; shift
    local started=$SECONDS index=0 code=0 elapsed available label pulse i
    local -a frames=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
    [[ "$MARK_OK" == '+' ]] && frames=('|' '/' '-' '\')
    UI_LOG="$(mktemp "${TMPDIR:-/tmp}/shopbot-step.XXXXXXXX")"
    if (( ! UI_ANIMATE )); then info "$message …"; fi
    # Reset inherited UI traps inside the worker so it cannot redraw the terminal.
    ( trap - EXIT ERR INT TERM; "$@" ) >"$UI_LOG" 2>&1 &
    UI_PID=$!
    if (( UI_ANIMATE )); then
        printf '\033[?25l'
        while kill -0 "$UI_PID" 2>/dev/null; do
            ui_width
            elapsed=$((SECONDS-started))
            available=$((UI_COLS-25)); (( available < 4 )) && available=4
            label="$message"; if (( ${#label} > available )); then label="${label:0:available-1}…"; fi
            pulse=''
            for ((i=0; i<5; i++)); do
                if (( i == index % 5 )); then pulse+="$BAR_ON"; else pulse+="$BAR_OFF"; fi
            done
            printf '\r\033[2K  %b%s%b  %s  %b%s  %02d:%02d%b' \
                "$ACCENT" "${frames[index % ${#frames[@]}]}" "$NC" "$label" \
                "$DIM" "$pulse" "$((elapsed/60))" "$((elapsed%60))" "$NC"
            index=$((index+1))
            sleep 0.1
        done
    fi
    wait "$UI_PID" || code=$?
    UI_PID=''
    if (( UI_ANIMATE )); then printf '\r\033[2K\033[?25h'; fi
    elapsed=$((SECONDS-started))
    if (( code == 0 )); then
        printf '  %b%s%b  %s  %b%02d:%02d%b\n' "$GREEN" "$MARK_OK" "$NC" "$message" "$DIM" "$((elapsed/60))" "$((elapsed%60))" "$NC"
    else
        UI_ERRORS=$((UI_ERRORS+1))
        printf '  %b%s%b  %s  %b[код %s]%b\n' "$RED" "$MARK_FAIL" "$NC" "$message" "$RED" "$code" "$NC" >&2
        printf '\n  %bПоследние строки вывода:%b\n' "$DIM" "$NC" >&2
        tail -n 16 "$UI_LOG" >&2 || true
        printf '\n' >&2
    fi
    rm -f -- "$UI_LOG"; UI_LOG=''
    return "$code"
}

ui_finish() {
    local title="$1" elapsed=$((SECONDS-UI_STARTED))
    printf '\n'
    rule
    if (( UI_ERRORS > 0 )); then
        printf '\n  %b!  ВЫПОЛНЕНО С ОШИБКАМИ%b\n' "$BOLD$YELLOW" "$NC"
        info 'Проверьте неуспешные операции выше.'
    else
        printf '\n  %b%s  %s%b\n' "$BOLD$GREEN" "$MARK_OK" "$title" "$NC"
    fi
    printf '  %bВремя %02d:%02d · замечаний %s · ошибок %s%b\n\n' \
        "$DIM" "$((elapsed/60))" "$((elapsed%60))" "$UI_WARNINGS" "$UI_ERRORS" "$NC"
    rule
    printf '\n'
}

ui_demo() {
    if [[ "$UI_KIND" == uninstall ]]; then
        print_banner
        section '1/3' 'Выбор компонентов'
        ui_menu_cards
        info 'Демонстрация варианта 2 · данные сохраняются'
        section '2/3' 'Удаление приложения'
        spinner_run 'Остановка контейнеров' sleep 1.2
        spinner_run 'Удаление агента обновления' sleep 0.8
        spinner_run 'Удаление файлов приложения' sleep 0.8
        section '3/3' 'Проверка результата'
        ok 'Каталог данных сохранён'
        ui_bar 3 3
        ui_finish 'ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА'
    else
        print_banner 'НОВАЯ УСТАНОВКА' 'DEMO'
        section '1/3' 'Подготовка окружения'
        spinner_run 'Проверка системных зависимостей' sleep 1.2
        ok 'Docker · Compose · Nginx'
        section '2/3' 'Настройка проекта'
        kv 'Домен' 'shop.example.com'
        spinner_run 'Настройка HTTPS и сертификата' sleep 1.2
        section '3/3' 'Запуск приложения'
        spinner_run 'Сборка и запуск контейнеров' sleep 1.4
        ui_bar 3 3
        ui_finish 'ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА'
        kv 'Пример адреса панели' 'https://shop.example.com:8443/login'
    fi
    info 'Демонстрация завершена. Система не изменена.'
    printf '\n'
}


ask_yes() {
    local prompt="$1"
    local default_answer="$2"
    local reply=""
    printf '%b  ? %b%s ' "$CYAN" "$NC" "$prompt"
    ui_read reply
    printf '\n'
    if [[ -z "$reply" ]]; then
        reply="$default_answer"
    fi
    [[ "$reply" =~ ^([yY]|[yY][eE][sS]|[дД]|[дД][аА])$ ]]
}

ask_confirm_word() {
    local word="$1"
    local answer=""
    printf '%b  ? %bВведите %b%s%b для подтверждения: ' "$CYAN" "$NC" "$BOLD$RED" "$word" "$NC"
    ui_read answer
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

ui_menu_cards() {
    printf '\n  %bВЫБЕРИТЕ СЦЕНАРИЙ%b\n\n' "$BOLD$WHITE" "$NC"
    printf '  %b01%b  %bСеть и сертификат%b\n' "$ACCENT" "$NC" "$BOLD" "$NC"
    printf '      %bКонфигурация Nginx · SSL домена%b\n\n' "$DIM" "$NC"
    printf '  %b02%b  %bУдалить приложение%b\n' "$ACCENT" "$NC" "$BOLD" "$NC"
    printf '      %bСеть · контейнеры · образы · код · агент%b\n' "$DIM" "$NC"
    printf '      %bБаза и настройки сохраняются%b\n\n' "$GREEN" "$NC"
    printf '  %b03%b  %bПриложение и Certbot%b\n' "$ACCENT" "$NC" "$BOLD" "$NC"
    printf '      %bВариант 02 + общий Certbot сервера%b\n\n' "$DIM" "$NC"
    printf '  %b04%b  %bВыбрать компоненты вручную%b\n' "$ACCENT" "$NC" "$BOLD" "$NC"
    printf '      %bОтдельное решение для каждого компонента%b\n\n' "$DIM" "$NC"
    printf '  %b00  Выйти без изменений%b\n\n' "$DIM" "$NC"
}

show_menu() {
    ui_menu_cards
    printf '%b  ? %bВыберите вариант [0-4] (по умолчанию 0): ' "$CYAN" "$NC"
    ui_read CHOICE
    printf '\n'
    if [[ -z "$CHOICE" ]]; then
        CHOICE=0
    fi

    case "$CHOICE" in
        1|01)
            REMOVE_CONFIG=true
            REMOVE_SSL=true
            ;;
        2|02)
            REMOVE_CONFIG=true
            REMOVE_SSL=true
            REMOVE_DOCKER=true
            REMOVE_PROJECT=true
            REMOVE_AGENT=true
            ;;
        3|03)
            REMOVE_CONFIG=true
            REMOVE_SSL=true
            REMOVE_DOCKER=true
            REMOVE_PROJECT=true
            REMOVE_AGENT=true
            REMOVE_CERTBOT=true
            ;;
        4|04)
            show_custom_menu
            ;;
        0|00)
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
    if [[ "$REMOVE_CERTBOT" == "true" ]]; then
        kv "Certbot" "удалить"
        warn "Certbot общий для сервера: его удаление затронет продление сертификатов других сайтов."
    fi
    printf '\n'

    if [[ "$CHOICE" == "1" || "$CHOICE" == "01" ]]; then
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
    ui_bar 5 5
    ui_finish 'ОБРАБОТКА ВЫБРАННЫХ КОМПОНЕНТОВ ЗАВЕРШЕНА'
    if [[ -d "$DATADIR" ]]; then
        kv "Данные сохранены" "$DATADIR"
    fi
    kv "Проект" "github.com/CyberERROR/remnawave-shopbot"
    printf '\n'
}

ui_init "$@"
if (( UI_DEMO )); then ui_demo; exit 0; fi

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
