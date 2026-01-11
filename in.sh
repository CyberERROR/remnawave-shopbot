#!/usr/bin/env bash
set -Eeuo pipefail

# Цветовая палитра
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# Настройки проекта
REPO_URL="https://github.com/CyberERROR/remnawave-shopbot.git"
PROJECT_DIR="remnawave-shopbot"
NGINX_CONF="/etc/nginx/sites-available/${PROJECT_DIR}.conf"
NGINX_LINK="/etc/nginx/sites-enabled/${PROJECT_DIR}.conf"

# Инициализация переменных
USER_DOMAIN_INPUT=""
DOMAIN=""
EMAIL=""
YOOKASSA_PORT=""

# --- Функции отображения ---

show_header() {
    clear
    echo -e "${PURPLE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${PURPLE}${BOLD}           REMNAWAVE SHOPBOT INSTALLER & UPDATER              ${NC}"
    echo -e "${PURPLE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

log_info() {
    echo -e "${BLUE}${BOLD}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}${BOLD}[✔]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}${BOLD}[!]${NC} $1"
}

log_error() {
    echo -e "${RED}${BOLD}[✘] Ошибка:${NC} $1" >&2
}

log_input() {
    echo -ne "${CYAN}${BOLD}[?]${NC} $1"
}

# Функция спиннера
run_with_spinner() {
    local message="$1"
    shift
    local cmd=("$@")

    echo -ne "${BLUE}${BOLD}[➜]${NC} ${message}... "

    local temp_log
    temp_log=$(mktemp)
    
    "${cmd[@]}" > "$temp_log" 2>&1 &
    local pid=$!
    
    local delay=0.1
    local spinstr='|/-\'
    
    tput civis 2>/dev/null || true

    while ps -p "$pid" > /dev/null 2>&1; do
        local temp=${spinstr#?}
        printf "[%c]" "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b"
    done

    wait "$pid"
    local exit_code=$?

    tput cnorm 2>/dev/null || true
    printf "   \b\b\b"

    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}${BOLD}OK${NC}"
        rm -f "$temp_log"
        return 0
    else
        echo -e "${RED}${BOLD}FAILED${NC}"
        echo -e "\n${RED}================ LOG OUTPUT =================${NC}"
        cat "$temp_log"
        echo -e "${RED}=============================================${NC}"
        rm -f "$temp_log"
        log_error "Процесс завершился с ошибкой (код $exit_code)."
        exit $exit_code
    fi
}

on_error() {
    tput cnorm 2>/dev/null || true
    echo ""
    log_error "Скрипт прерван на строке $1."
    exit 1
}
trap 'on_error $LINENO' ERR

# --- Утилиты ---

sanitize_domain() {
    if [[ -z "${1:-}" ]]; then
        echo ""
        return
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
    ip=$(hostname -I 2>/dev/null | awk '{print $1}')
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

# --- Основные этапы ---

ensure_sudo_refresh() {
    if ! sudo -v; then
        log_error "Требуются права sudo. Введите пароль выше."
        exit 1
    fi
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
    
    for cmd in "${!packages[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("${packages[$cmd]}")
        fi
    done

    if ((${#missing[@]})); then
        run_with_spinner "Установка системных зависимостей (${#missing[@]} шт.)" \
            sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get update -qq && apt-get install -y --no-install-recommends ${missing[*]}"
    else
        log_success "Системные зависимости в порядке"
    fi
}

ensure_services() {
    run_with_spinner "Проверка и запуск сервисов (Docker, Nginx)" \
        sudo bash -c "systemctl enable --now docker nginx"
}

ensure_certbot_nginx() {
    if command -v certbot >/dev/null 2>&1 && certbot plugins 2>/dev/null | grep -qi 'nginx'; then
        log_success "Плагин Certbot Nginx активен"
        return
    fi

    run_with_spinner "Установка плагина Certbot Nginx" \
        sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get update -qq && apt-get install -y --no-install-recommends python3-certbot-nginx || (snap install core 2>/dev/null; snap refresh core 2>/dev/null; snap install --classic certbot 2>/dev/null; ln -sf /snap/bin/certbot /usr/bin/certbot)"
    
    if certbot plugins 2>/dev/null | grep -qi 'nginx'; then
        log_success "Плагин Certbot Nginx успешно настроен"
    else
        log_error "Не удалось автоматически установить плагин Nginx для Certbot."
        exit 1
    fi
}

configure_nginx() {
    local domain="$1"
    local port="$2"
    
    log_info "Генерация конфигурации Nginx..."
    
    sudo tee "$NGINX_CONF" >/dev/null <<EOF
server {
    listen ${port} ssl http2;
    listen [::]:${port} ssl http2;
    server_name ${domain};

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
        sudo ln -s "$NGINX_CONF" "$NGINX_LINK"
    fi

    run_with_spinner "Проверка и перезагрузка Nginx" \
        sudo bash -c "nginx -t && systemctl reload nginx"
}

# --- Начало выполнения ---

show_header
ensure_sudo_refresh

# Режим обновления
if [[ -f "$NGINX_CONF" ]]; then
    log_info "Обнаружена существующая конфигурация."
    
    if [[ ! -d "$PROJECT_DIR" ]]; then
        log_error "Каталог проекта не найден ($PROJECT_DIR). Удалите $NGINX_CONF для чистой установки."
        exit 1
    fi
    
    cd "$PROJECT_DIR"
    run_with_spinner "Получение обновлений из Git" git pull --ff-only
    
    run_with_spinner "Пересборка контейнеров" \
        sudo bash -c "docker-compose down --remove-orphans && docker-compose up -d --build"
    
    echo ""
    log_success "Обновление завершено!"
    exit 0
fi

# Режим установки
log_info "Инициализация процесса установки..."
echo ""

ensure_packages
ensure_services
ensure_certbot_nginx

if [[ ! -d "$PROJECT_DIR/.git" ]]; then
    run_with_spinner "Клонирование репозитория" git clone "$REPO_URL" "$PROJECT_DIR"
else
    log_warn "Каталог проекта уже существует, пропускаем клонирование"
fi
cd "$PROJECT_DIR"

echo ""

# Ввод домена
while [[ -z "$DOMAIN" ]]; do
    log_input "Введите ваш домен (без http/s): "
    read -r USER_DOMAIN_INPUT || true
    DOMAIN=$(sanitize_domain "$USER_DOMAIN_INPUT")
    if [[ -z "${DOMAIN:-}" ]]; then
        log_warn "Домен не может быть пустым."
    fi
done

# Ввод Email
while [[ -z "$EMAIL" ]]; do
    log_input "Введите Email для SSL (Let's Encrypt): "
    read -r EMAIL || true
    if [[ -z "${EMAIL:-}" ]]; then
        log_warn "Email обязателен."
    fi
done
echo ""

# Проверка IP
SERVER_IP=$(get_server_ip || true)
DOMAIN_IP=$(resolve_domain_ip "$DOMAIN" || true)

if [[ -n "$SERVER_IP" ]] && [[ -n "$DOMAIN_IP" ]] && [[ "$SERVER_IP" != "$DOMAIN_IP" ]]; then
    log_warn "Внимание: IP сервера ($SERVER_IP) отличается от IP домена ($DOMAIN_IP)."
    log_warn "Это может привести к ошибке выдачи SSL сертификата."
    
    while true; do
        log_input "Продолжить все равно? (y/n): "
        read -r -n1 REPLY || true
        echo ""
        case "$REPLY" in
            [yY]) break ;;
            [nN]) log_error "Установка отменена."; exit 1 ;;
            *) ;;
        esac
    done
fi

echo ""

# Firewall
if command -v ufw >/dev/null 2>&1 && sudo ufw status | grep -q 'Status: active'; then
    run_with_spinner "Настройка Firewall (UFW)" \
        sudo bash -c "ufw allow 80/tcp && ufw allow 443/tcp && ufw allow 1488/tcp && ufw allow 8443/tcp"
fi

# SSL
if [[ -d "/etc/letsencrypt/live/${DOMAIN}" ]]; then
    log_success "SSL сертификаты уже существуют"
else
    run_with_spinner "Выпуск SSL сертификата (Let's Encrypt)" \
        sudo bash -c "certbot --nginx -d $DOMAIN --email $EMAIL --agree-tos --non-interactive --redirect"
fi

echo ""
log_input "Порт для вебхуков YooKassa [443/8443] (default: 8443): "
read -r YOOKASSA_PORT_INPUT || true
YOOKASSA_PORT="${YOOKASSA_PORT_INPUT:-8443}"

if [[ "$YOOKASSA_PORT" != "443" && "$YOOKASSA_PORT" != "8443" ]]; then
    YOOKASSA_PORT=8443
fi

echo ""
configure_nginx "$DOMAIN" "$YOOKASSA_PORT"

run_with_spinner "Сборка и запуск Docker контейнеров" \
    sudo bash -c "if [ -n \"\$(docker-compose ps -q 2>/dev/null)\" ]; then docker-compose down; fi; docker-compose up -d --build"

echo ""
echo -e "${GREEN}┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓${NC}"
echo -e "${GREEN}┃                  УСТАНОВКА ЗАВЕРШЕНА!                        ┃${NC}"
echo -e "${GREEN}┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛${NC}"
echo ""
echo -e " ${BOLD}Адрес панели:${NC}     https://${DOMAIN}:${YOOKASSA_PORT}/login"
echo -e " ${BOLD}Данные входа:${NC}     ${CYAN}admin${NC} / ${CYAN}admin${NC}"
echo -e " ${BOLD}Webhook URL:${NC}      https://${DOMAIN}:${YOOKASSA_PORT}/yookassa-webhook"
echo ""
echo -e "${YELLOW} ⚠  Пожалуйста, смените пароль сразу после входа!${NC}"
echo ""
