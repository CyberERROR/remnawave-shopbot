#!/usr/bin/env bash
set -Eeuo pipefail

BRAND_NAME="remnawave-shopbot"
REPO_URL="https://github.com/CyberERROR/remnawave-shopbot.git"
PROJECT_NAME="remnawave-shopbot"
PROJECT_DIR="$HOME/$PROJECT_NAME"

if [[ ! -d "$PROJECT_DIR" ]] && [[ -d "./$PROJECT_NAME" ]]; then
    PROJECT_DIR="$(pwd)/$PROJECT_NAME"
fi

DATADIR="$(dirname "$PROJECT_DIR")/remnawave-shopbot-data"
NGINX_CONF="/etc/nginx/sites-available/${PROJECT_NAME}.conf"
NGINX_LINK="/etc/nginx/sites-enabled/${PROJECT_NAME}.conf"

USER_DOMAIN_INPUT=""
DOMAIN=""
YOOKASSA_PORT=""
SSL_EMAIL=""
SERVER_IP=""
DOMAIN_IP=""
MODE="install"

UI_KIND="install"
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


sanitize_domain() {
    if [[ -z "${1:-}" ]]; then
        echo ""
        return 0
    fi
    echo "$1" | sed -e 's%^https\?://%%' -e 's%/.*$%%' | tr -cd 'A-Za-z0-9.-' | tr '[:upper:]' '[:lower:]'
}

get_server_ip() {
    local ipv4_re='^([0-9]{1,3}\.){3}[0-9]{1,3}$'
    local ip
    for url in "https://api.ipify.org" "https://ifconfig.co/ip" "https://ipv4.icanhazip.com"; do
        ip=$(curl -fsS --max-time 3 "$url" 2>/dev/null | tr -d '\r\n\t ') || true
        if [[ $ip =~ $ipv4_re ]]; then
            echo "$ip"
            return 0
        fi
    done
    ip=$(hostname -I 2>/dev/null | awk '{print $1}' || true)
    if [[ $ip =~ $ipv4_re ]]; then echo "$ip"; fi
}

resolve_domain_ip() {
    local domain="${1:-}"
    [[ -z "$domain" ]] && return 1
    local ipv4_re='^([0-9]{1,3}\.){3}[0-9]{1,3}$'
    local ip
    ip=$(getent ahostsv4 "$domain" 2>/dev/null | awk '{print $1}' | head -n1)
    if [[ $ip =~ $ipv4_re ]]; then echo "$ip"; return 0; fi
    if command -v dig >/dev/null 2>&1; then
        ip=$(dig +short A "$domain" 2>/dev/null | grep -E "$ipv4_re" | head -n1)
        if [[ $ip =~ $ipv4_re ]]; then echo "$ip"; return 0; fi
    fi
    return 1
}

get_domain_from_nginx() {
    if [[ -f "$NGINX_CONF" ]]; then
        grep "server_name" "$NGINX_CONF" | awk '{print $2}' | sed 's/;//' | head -n1
    fi
}

get_port_from_nginx() {
    if [[ -f "$NGINX_CONF" ]]; then
        grep "listen" "$NGINX_CONF" | head -n1 | awk '{print $2}' | sed 's/;//'
    fi
}

get_project_version() {
    local file="$PROJECT_DIR/pyproject.toml"
    if [[ -f "$file" ]]; then
        grep -m1 '^version' "$file" | sed -e 's/^version[[:space:]]*=[[:space:]]*"\(.*\)".*/\1/' || true
    fi
}

build_ssl_email() {
    local domain="${1:-}"
    if [[ -z "$domain" ]]; then
        echo ""
        return 0
    fi
    echo "admin@${domain}"
}

ensure_packages() {
    declare -A packages=(
        [git]='git'
        [docker]='docker.io'
        [docker-compose]='docker-compose'
        [nginx]='nginx'
        [curl]='curl'
        [certbot]='certbot'
        [dig]='dnsutils'
    )
    local missing=()
    local missing_desc=()

    for cmd in "${!packages[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("${packages[$cmd]}")
            missing_desc+=("$cmd")
        fi
    done

    if ((${#missing[@]})); then
        info "Не хватает: ${missing_desc[*]}"
        spinner_run "Установка зависимостей ($(echo "${missing[@]}" | tr ' ' ',' | sed 's/,/, /g'))" \
            sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get update -qq && apt-get install -y --no-install-recommends ${missing[*]}" || {
            fail "Не удалось установить зависимости"
            exit 1
        }
    else
        ok "Системные зависимости уже установлены"
    fi
}

ensure_services() {
    spinner_run "Включение Docker" sudo systemctl enable docker || true
    spinner_run "Запуск Docker" sudo systemctl start docker || true
    spinner_run "Включение Nginx" sudo systemctl enable nginx || true
    spinner_run "Запуск Nginx" sudo systemctl start nginx || true
}

ensure_certbot_nginx() {
    if command -v certbot >/dev/null 2>&1 && certbot plugins 2>/dev/null | grep -qi 'nginx'; then
        ok "Плагин Certbot Nginx активен"
        return 0
    fi

    info "Установка плагина Certbot для Nginx..."
    spinner_run "Установка python3-certbot-nginx" \
        sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get update -qq && apt-get install -y --no-install-recommends python3-certbot-nginx" || {
        warn "Попытка установки через snap..."
        if ! command -v snap >/dev/null 2>&1; then
            spinner_run "Установка snapd" \
                sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get update -qq && apt-get install -y --no-install-recommends snapd" || {
                fail "Не удалось установить snapd"
                exit 1
            }
        fi
        spinner_run "Установка Certbot через snap" \
            sudo bash -c "snap install core 2>/dev/null; snap refresh core 2>/dev/null; snap install --classic certbot 2>/dev/null; ln -sf /snap/bin/certbot /usr/bin/certbot" || {
            fail "Не удалось установить Certbot"
            exit 1
        }
    }

    if certbot plugins 2>/dev/null | grep -qi 'nginx'; then
        ok "Плагин Certbot Nginx готов"
    else
        fail "Не удалось установить плагин Nginx для Certbot"
        exit 1
    fi
}

configure_nginx() {
    local domain="$1"
    local port="$2"

    info "Настройка Nginx для ${domain}:${port}"

    sudo tee "$NGINX_CONF" >/dev/null <<EOF
server {
    listen ${port} ssl http2;
    listen [::]:${port} ssl http2;
    server_name ${domain};
    client_max_body_size 500M;

    ssl_certificate /etc/letsencrypt/live/${domain}/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/${domain}/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

    location / {
        proxy_pass http://127.0.0.1:1488;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

    if [[ ! -L "$NGINX_LINK" ]]; then
        sudo ln -s "$NGINX_CONF" "$NGINX_LINK" 2>/dev/null || true
    fi

    spinner_run "Проверка конфигурации Nginx" sudo nginx -t || {
        fail "Некорректная конфигурация Nginx"
        exit 1
    }

    spinner_run "Перезагрузка Nginx" sudo systemctl reload nginx || {
        fail "Не удалось перезагрузить Nginx"
        exit 1
    }
}

show_docker_images() {
    if command -v docker >/dev/null 2>&1 && [[ -f "docker-compose.yml" ]]; then
        info "Docker контейнеры в проекте:"
        docker-compose config --services 2>/dev/null | sed 's/^/     - /' || true
    fi
}

prepare_data_dir() {
    if [[ ! -d "$DATADIR" ]]; then
        spinner_run "Создание каталога данных" mkdir -p "$DATADIR" || {
            fail "Не удалось создать каталог данных $DATADIR"
            exit 1
        }
    fi
    mkdir -p "$DATADIR/backups" "$DATADIR/update" 2>/dev/null || true

    local moved=0
    if [[ -f "$PROJECT_DIR/users.db" ]] && [[ ! -f "$DATADIR/users.db" ]]; then
        spinner_run "Перенос базы данных" mv -f "$PROJECT_DIR/users.db" "$DATADIR/users.db" || warn "Не удалось перенести users.db"
        moved=1
    fi
    if [[ -f "$PROJECT_DIR/.env" ]] && [[ ! -f "$DATADIR/.env" ]]; then
        spinner_run "Перенос файла .env" mv -f "$PROJECT_DIR/.env" "$DATADIR/.env" || warn "Не удалось перенести .env"
        moved=1
    fi
    if [[ -d "$PROJECT_DIR/backups" ]] && [[ ! -d "$DATADIR/backups" ]]; then
        spinner_run "Перенос резервных копий" mv -f "$PROJECT_DIR/backups" "$DATADIR/backups" || warn "Не удалось перенести backups"
        moved=1
    elif [[ -d "$PROJECT_DIR/backups" ]] && [[ -n "$(ls -A "$PROJECT_DIR/backups" 2>/dev/null)" ]]; then
        spinner_run "Перенос старых копий" bash -c "cp -a '$PROJECT_DIR/backups/.' '$DATADIR/backups/'" || true
        moved=1
    fi

    if [[ "$moved" -eq 1 ]]; then
        ok "База и настройки перенесены в ${DATADIR}"
    else
        ok "Каталог данных готов: ${DATADIR}"
    fi
}

backup_data() {
    if [[ -f "$DATADIR/users.db" ]]; then
        local stamp
        stamp=$(date '+%Y%m%d-%H%M%S')
        spinner_run "Резервная копия базы перед обновлением" \
            cp -f "$DATADIR/users.db" "$DATADIR/backups/pre-update-$stamp.db" || warn "Не удалось создать резервную копию базы"
    fi
}

install_update_agent() {
    if [[ ! -f "$PROJECT_DIR/deploy/shopbot-update-agent.sh" ]]; then
        warn "Агент обновления не найден, кнопка «Обновить» будет недоступна"
        return 0
    fi
    spinner_run "Установка агента обновления" sudo install -m 0755 "$PROJECT_DIR/deploy/shopbot-update-agent.sh" /usr/local/bin/shopbot-update-agent || {
        warn "Не удалось установить агент обновления"
        return 0
    }
    sudo tee /etc/default/shopbot-update >/dev/null <<EOF
SHOPBOT_PROJECT_DIR=$PROJECT_DIR
SHOPBOT_DATA_DIR=$DATADIR
SHOPBOT_GIT_BRANCH=main
EOF
    sudo install -m 0644 "$PROJECT_DIR/deploy/shopbot-update.service" /etc/systemd/system/shopbot-update.service || true
    sudo install -m 0644 "$PROJECT_DIR/deploy/shopbot-update.timer" /etc/systemd/system/shopbot-update.timer || true
    sudo systemctl daemon-reload 2>/dev/null || true
    sudo systemctl enable --now shopbot-update.timer 2>/dev/null || warn "Не удалось включить таймер обновления"
    ok "Кнопка «Обновить» в панели активирована"
}

ask_domain() {
    DOMAIN=""
    while [[ -z "$DOMAIN" ]]; do
        printf '%b  ? %bВведите домен (без http/s): ' "$CYAN" "$NC"
        ui_read USER_DOMAIN_INPUT
        if [[ -z "$USER_DOMAIN_INPUT" ]]; then
            warn "Домен не может быть пустым."
            continue
        fi
        DOMAIN=$(sanitize_domain "$USER_DOMAIN_INPUT")
        if [[ -z "$DOMAIN" ]]; then
            warn "Домен содержит недопустимые символы."
            DOMAIN=""
        fi
    done
}

ask_port() {
    local default_port="${1:-8443}"
    printf '%b  ? %bПорт для вебхуков YooKassa [1-65535] (по умолчанию %s): ' "$CYAN" "$NC" "$default_port"
    ui_read YOOKASSA_PORT_INPUT
    YOOKASSA_PORT="${YOOKASSA_PORT_INPUT:-$default_port}"
    if ! [[ "$YOOKASSA_PORT" =~ ^[0-9]+$ ]] || (( YOOKASSA_PORT < 1 || YOOKASSA_PORT > 65535 )); then
        fail "Порт должен быть целым числом от 1 до 65535."
        exit 1
    fi
    ok "Порт вебхуков: ${YOOKASSA_PORT}"
}

check_ip_match() {
    SERVER_IP=$(get_server_ip || true)
    DOMAIN_IP=$(resolve_domain_ip "$DOMAIN" || true)
    if [[ -n "$SERVER_IP" ]]; then
        kv "IP сервера" "$SERVER_IP"
    fi
    if [[ -n "$DOMAIN_IP" ]]; then
        kv "IP домена" "$DOMAIN_IP"
    fi
    if [[ -n "$SERVER_IP" ]] && [[ -n "$DOMAIN_IP" ]] && [[ "$SERVER_IP" != "$DOMAIN_IP" ]]; then
        warn "IP сервера ($SERVER_IP) отличается от IP домена ($DOMAIN_IP)."
        warn "SSL может не выпуститься."
        local reply=""
        while true; do
            printf '%b  ? %bПродолжить всё равно? (y/n): ' "$CYAN" "$NC"
            ui_read reply
            printf '\n'
            case "$reply" in
                [yY]) break ;;
                [nN]) fail "Установка отменена."; exit 1 ;;
                *) ;;
            esac
        done
    fi
}

issue_certificate() {
    SSL_EMAIL=$(build_ssl_email "$DOMAIN")
    if [[ -d "/etc/letsencrypt/live/${DOMAIN}" ]]; then
        ok "SSL-сертификат уже существует для ${DOMAIN}"
        return 0
    fi
    info "Почта для Let's Encrypt: ${SSL_EMAIL} (создана из домена)"
    spinner_run "Получение SSL-сертификата (может занять время)" \
        sudo bash -c "certbot --nginx -d $DOMAIN --email $SSL_EMAIL --agree-tos --non-interactive --redirect --no-eff-email 2>&1" || {
        fail "Не удалось получить SSL-сертификат. Проверьте:"
        warn "домен указан верно и смотрит на этот сервер ($SERVER_IP)"
        warn "порты 80 и 443 открыты"
        exit 1
    }
}

configure_firewall() {
    if command -v ufw >/dev/null 2>&1 && sudo ufw status 2>/dev/null | grep -q 'Status: active'; then
        spinner_run "Открытие портов UFW (80, 443, 1488, ${YOOKASSA_PORT})" \
            sudo bash -c "ufw allow 80/tcp && ufw allow 443/tcp && ufw allow 1488/tcp && ufw allow ${YOOKASSA_PORT}/tcp" || warn "Не удалось настроить UFW"
    fi
}

final_card() {
    local mode="$1" domain="$2" port="$3" version="$4"
    local title="УСТАНОВКА ЗАВЕРШЕНА" total=9
    if [[ "$mode" == update ]]; then title="ОБНОВЛЕНИЕ ЗАВЕРШЕНО"; total=7; fi
    ui_bar "$total" "$total"
    ui_finish "$title"
    [[ -z "$version" ]] || kv "Версия" "$BRAND_NAME $version"
    if [[ -n "$domain" && -n "$port" ]]; then
        kv "Панель управления" "https://${domain}:${port}/login"
        if [[ "$mode" == install ]]; then
            kv "Первый вход" "admin / admin"
            warn "Смените пароль admin сразу после первого входа."
        else
            info "Для входа используйте действующие учётные данные."
        fi
        kv "Webhook YooKassa" "https://${domain}:${port}/yookassa-webhook"
    fi
    kv "Каталог данных" "$DATADIR"
    kv "Обновления" "кнопка «Обновить» в панели"
    printf '\n'
}

run_update_mode() {
    local version="$1"
    print_banner "РЕЖИМ ОБНОВЛЕНИЯ" "$version"

    section "1/7" "Проверка проекта"
    info "Запрос прав администратора"
    if ! sudo -v; then
        fail "Требуются права sudo"
        exit 1
    fi
    if [[ ! -d "$PROJECT_DIR" ]]; then
        warn "Папка проекта не найдена, восстанавливаю из репозитория"
        spinner_run "Клонирование репозитория" git clone "$REPO_URL" "$PROJECT_DIR" || {
            fail "Не удалось клонировать репозиторий в $PROJECT_DIR"
            exit 1
        }
    else
        ok "Проект найден: ${PROJECT_DIR}"
    fi
    cd "$PROJECT_DIR"

    section "2/7" "Параметры установки"
    DOMAIN=$(get_domain_from_nginx)
    YOOKASSA_PORT=$(get_port_from_nginx)
    if [[ -z "$DOMAIN" ]]; then
        warn "Не удалось прочитать домен из Nginx"
        ask_domain
    else
        ok "Домен: ${DOMAIN}"
    fi
    if [[ -z "$YOOKASSA_PORT" ]]; then
        YOOKASSA_PORT="8443"
    fi
    ok "Порт вебхуков: ${YOOKASSA_PORT}"

    section "3/7" "Остановка контейнеров"
    spinner_run "Остановка контейнеров" sudo bash -c "docker-compose down --remove-orphans" || true

    section "4/7" "Данные и резервная копия"
    prepare_data_dir
    backup_data

    section "5/7" "Обновление файлов"
    spinner_run "Загрузка новой версии" bash -c "git fetch origin main && git reset --hard origin/main" || {
        fail "Не удалось обновить репозиторий"
        exit 1
    }
    version=$(get_project_version)
    ok "Новая версия: ${version:-неизвестна}"

    section "6/7" "Пересборка контейнеров"
    show_docker_images
    ensure_services
    spinner_run "Пересборка и запуск" sudo bash -c "docker-compose down --remove-orphans && docker-compose up -d --build" || {
        fail "Не удалось пересобрать контейнеры"
        exit 1
    }

    section "7/7" "Агент обновления"
    install_update_agent

    final_card "update" "$DOMAIN" "$YOOKASSA_PORT" "$version"
    exit 0
}

run_install_mode() {
    print_banner "НОВАЯ УСТАНОВКА" ""

    section "1/9" "Проверка окружения"
    info "Запрос прав администратора"
    if ! sudo -v; then
        fail "Требуются права sudo"
        exit 1
    fi
    ok "Права подтверждены"

    section "2/9" "Системные зависимости"
    ensure_packages
    ensure_services
    ensure_certbot_nginx

    section "3/9" "Развёртывание проекта"
    if [[ ! -d "$PROJECT_DIR/.git" ]]; then
        spinner_run "Клонирование репозитория" git clone "$REPO_URL" "$PROJECT_DIR" || {
            fail "Не удалось клонировать репозиторий"
            exit 1
        }
    else
        warn "Каталог проекта уже существует, клонирование пропущено"
    fi
    cd "$PROJECT_DIR"
    local version
    version=$(get_project_version)
    ok "Версия: ${version:-неизвестна}"

    section "4/9" "Подготовка данных"
    if [[ -f docker-compose.yml ]]; then
        spinner_run "Остановка старых контейнеров" sudo bash -c "docker-compose down --remove-orphans" || true
    fi
    prepare_data_dir

    section "5/9" "Параметры"
    ask_domain
    check_ip_match

    section "6/9" "SSL-сертификат"
    issue_certificate

    section "7/9" "Порт и сеть"
    ask_port 8443
    configure_firewall
    configure_nginx "$DOMAIN" "$YOOKASSA_PORT"

    section "8/9" "Сборка и запуск"
    show_docker_images
    spinner_run "Сборка и запуск контейнеров" \
        sudo bash -c "if [ -n \"\$(docker-compose ps -q 2>/dev/null)\" ]; then docker-compose down --remove-orphans; fi; docker-compose up -d --build" || {
        fail "Не удалось запустить Docker контейнеры"
        exit 1
    }

    section "9/9" "Агент обновления"
    install_update_agent

    final_card "install" "$DOMAIN" "$YOOKASSA_PORT" "$version"
}

ui_init "$@"
if (( UI_DEMO )); then ui_demo; exit 0; fi

if [[ -f "$NGINX_CONF" ]]; then
    MODE="update"
fi

if [[ "$MODE" == "update" ]]; then
    run_update_mode "$(get_project_version)"
else
    run_install_mode
fi
