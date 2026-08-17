#!/usr/bin/env bash
set -Eeuo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Имя конфига Nginx для Webapp
WEBAPP_CONF_NAME="remnawave-webapp"
NGINX_CONF="/etc/nginx/sites-available/${WEBAPP_CONF_NAME}.conf"
NGINX_LINK="/etc/nginx/sites-enabled/${WEBAPP_CONF_NAME}.conf"
WEBAPP_PORT="8000" # Порт, на котором запущен Uvicorn

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

run_with_spinner() {
    local message="$1"
    shift
    local cmd=("$@")

    echo -ne "${BLUE}${BOLD}[➜]${NC} ${message}... "

    local temp_log
    temp_log=$(mktemp)
    
    if "${cmd[@]}" > "$temp_log" 2>&1; then
        echo -e "${GREEN}${BOLD}OK${NC}"
        rm -f "$temp_log"
        return 0
    else
        local exit_code=$?
        echo -e "${RED}${BOLD}FAILED${NC}"
        echo -e "\n${RED}================ LOG OUTPUT =================${NC}"
        cat "$temp_log"
        echo -e "${RED}=============================================${NC}"
        rm -f "$temp_log"
        return $exit_code
    fi
}

on_error() {
    echo ""
    log_error "Скрипт прерван на строке $1."
    exit 1
}
trap 'on_error $LINENO' ERR

sanitize_domain() {
    if [[ -z "${1:-}" ]]; then
        echo ""
        return 0
    fi
    echo "$1" | sed -e 's%^https\?://%%' -e 's%/.*$%%' | tr -cd 'A-Za-z0-9.-' | tr '[:upper:]' '[:lower:]'
}

get_server_ipv4() {
    local ip
    for service in https://api.ipify.org https://ifconfig.me/ip https://icanhazip.com; do
        ip=$(curl -4fsS --max-time 10 "$service" 2>/dev/null | tr -d '[:space:]') || continue
        if [[ "$ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
            echo "$ip"
            return 0
        fi
    done
    return 1
}

get_domain_ipv4() {
    getent ahostsv4 "$1" 2>/dev/null | awk 'NR == 1 { print $1 }'
}

verify_domain_dns() {
    local domain="$1"
    local server_ip
    local domain_ip

    server_ip=$(get_server_ipv4) || {
        log_error "Не удалось автоматически определить публичный IPv4 сервера."
        return 1
    }
    domain_ip=$(get_domain_ipv4 "$domain") || true

    if [[ -z "$domain_ip" ]]; then
        log_error "Для ${domain} не найдена IPv4 A-запись. Создайте A-запись: ${domain} → ${server_ip}."
        return 1
    fi
    if [[ "$domain_ip" != "$server_ip" ]]; then
        log_error "A-запись ${domain} указывает на ${domain_ip}, но IPv4 этого сервера: ${server_ip}."
        log_info "Измените A-запись: ${domain} → ${server_ip}, дождитесь обновления DNS и запустите скрипт снова."
        return 1
    fi
    log_success "DNS проверен: ${domain} → ${server_ip}"
}

wait_for_package_manager() {
    local timeout=600
    local elapsed=0
    local locks=(/var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/lib/apt/lists/lock /var/cache/apt/archives/lock)

    while fuser "${locks[@]}" >/dev/null 2>&1; do
        if ((elapsed >= timeout)); then
            log_error "Менеджер пакетов занят более 10 минут. Дождитесь завершения системного обновления и запустите скрипт повторно."
            return 1
        fi
        log_warn "Ожидание завершения системного обновления: ${elapsed}/${timeout} сек."
        sleep 10
        ((elapsed += 10))
    done
}

ensure_packages() {
    wait_for_package_manager
    run_with_spinner "Обновление списка пакетов" sudo apt-get update -qq
    
    declare -A packages=( 
        [nginx]='nginx' 
        [certbot]='certbot' 
        [python3-certbot-nginx]='python3-certbot-nginx'
    )
    local missing=()
    local installed=()
    
    for cmd in "${!packages[@]}"; do
        if ! dpkg -l | grep -q "${packages[$cmd]}"; then
            missing+=("${packages[$cmd]}")
        else
            installed+=("${packages[$cmd]}")
        fi
    done

    if ((${#missing[@]})); then
        wait_for_package_manager
        run_with_spinner "Установка зависимостей (${missing[*]})" \
            sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get install -y --no-install-recommends ${missing[*]}"
    fi

    if ((${#installed[@]})); then
        local action=""
        while [[ "$action" != "1" && "$action" != "2" ]]; do
            echo ""
            log_info "Установленные зависимости: ${installed[*]}"
            echo "1) Пропустить установленные зависимости"
            echo "2) Обновить установленные зависимости"
            log_input "Выберите действие [1-2]: "
            read -r action < /dev/tty || true
            if [[ "$action" != "1" && "$action" != "2" ]]; then
                log_warn "Введите 1 или 2."
            fi
        done

        if [[ "$action" == "2" ]]; then
            wait_for_package_manager
            run_with_spinner "Обновление зависимостей (${installed[*]})" \
                sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get install -y --only-upgrade ${installed[*]}"
        else
            log_success "Установленные зависимости пропущены"
        fi
    elif (( ! ${#missing[@]} )); then
        log_success "Необходимые пакеты уже установлены"
    fi
}

ensure_services() {
    run_with_spinner "Проверка Nginx" sudo systemctl enable nginx --now || true
}

configure_nginx() {
    local domain="$1"
    
    log_info "Создание конфигурации Nginx для $domain..."
    
    sudo tee "$NGINX_CONF" >/dev/null <<EOF
server {
    listen 80;
    server_name ${domain};
    client_max_body_size 100M;

    location / {
        proxy_pass http://127.0.0.1:${WEBAPP_PORT};
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        
        # Websocket support (если нужно)
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
EOF
    
    if [[ ! -L "$NGINX_LINK" ]]; then
        sudo ln -s "$NGINX_CONF" "$NGINX_LINK" 2>/dev/null || true
    fi

    run_with_spinner "Проверка конфигурации Nginx" sudo nginx -t
    run_with_spinner "Перезагрузка Nginx" sudo systemctl reload nginx
}

obtain_ssl() {
    local domain="$1"
    local email="$2"
    
    if [[ -d "/etc/letsencrypt/live/${domain}" ]]; then
        log_success "SSL сертификат для $domain уже существует"
        return
    fi
    
    log_info "Получение SSL сертификата..."
    run_with_spinner "Certbot" \
        sudo certbot --nginx -d "$domain" --email "$email" --agree-tos --non-interactive --redirect --no-eff-email
}

# --- Main Script ---

clear
echo -e "${BLUE}${BOLD}=== Настройка Webapp (Nginx + SSL) ===${NC}"
echo ""

# Проверка прав root/sudo
if [ "$EUID" -ne 0 ]; then
    log_warn "Запуск через sudo..."
    exec sudo "$0" "$@"
fi

ensure_packages
ensure_services

DOMAIN=""
while [[ -z "$DOMAIN" ]]; do
    log_input "Введите домен для Webapp (например, app.example.com): "
    read -r USER_DOMAIN_INPUT < /dev/tty || true
    DOMAIN=$(sanitize_domain "$USER_DOMAIN_INPUT")
    if [[ -z "$DOMAIN" ]]; then
        log_warn "Некорректный домен."
    fi
done

verify_domain_dns "$DOMAIN"

EMAIL="admin@${DOMAIN}"
log_info "Email для SSL: ${EMAIL}"

echo ""
configure_nginx "$DOMAIN"
obtain_ssl "$DOMAIN" "$EMAIL"

echo ""
echo -e "${GREEN}${BOLD}Установка завершена!${NC}"
echo -e "Webapp доступен по адресу: ${CYAN}https://${DOMAIN}${NC}"
echo -e "Убедитесь, что в админ-панели включен Webapp."
echo ""
