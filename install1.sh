#!/usr/bin/env bash
set -Eeuo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

REPO_URL="https://github.com/CyberERROR/remnawave-shopbot.git"
PROJECT_DIR="remnawave-shopbot"
NGINX_CONF="/etc/nginx/sites-available/${PROJECT_DIR}.conf"
NGINX_LINK="/etc/nginx/sites-enabled/${PROJECT_DIR}.conf"

USER_DOMAIN_INPUT=""
DOMAIN=""
EMAIL=""
YOOKASSA_PORT=""

show_header() {
    clear
    echo -e "${PURPLE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${PURPLE}${BOLD}           REMNAWAVE SHOPBOT INSTALLER & UPDATER              ${NC}"
    echo -e "${PURPLE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

show_footer() {
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Репозиторий:${NC}  https://github.com/CyberERROR/remnawave-shopbot"
    echo -e "${CYAN}Telegram:${NC}      https://t.me/+7hUhNxAdzBpjNWRi"
    echo -e "${CYAN}Разработчик:${NC}   https://github.com/CyberERROR"
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
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

run_with_animated_spinner() {
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
        return $exit_code
    fi
}

on_error() {
    tput cnorm 2>/dev/null || true
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
    local missing_desc=()
    
    for cmd in "${!packages[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("${packages[$cmd]}")
            missing_desc+=("$cmd")
        fi
    done

    if ((${#missing[@]})); then
        echo -e "${BLUE}${BOLD}[➜]${NC} Требуются зависимости: ${CYAN}${missing_desc[*]}${NC}"
        run_with_animated_spinner "Установка зависимостей (${missing[*]})" \
            sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get update -qq && apt-get install -y --no-install-recommends ${missing[*]}" || {
            log_error "Не удалось установить зависимости"
            exit 1
        }
    else
        log_success "Системные зависимости в порядке"
    fi
}

ensure_services() {
    run_with_spinner "Проверка Docker" sudo systemctl enable docker || true
    run_with_spinner "Запуск Docker" sudo systemctl start docker || true
    run_with_spinner "Проверка Nginx" sudo systemctl enable nginx || true
    run_with_spinner "Запуск Nginx" sudo systemctl start nginx || true
}

ensure_certbot_nginx() {
    if command -v certbot >/dev/null 2>&1 && certbot plugins 2>/dev/null | grep -qi 'nginx'; then
        log_success "Плагин Certbot Nginx активен"
        return 0
    fi

    log_info "Установка плагина Certbot для Nginx..."
    
    run_with_animated_spinner "Установка python3-certbot-nginx" \
        sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get update -qq && apt-get install -y --no-install-recommends python3-certbot-nginx" || {
        log_warn "Попытка установки через snap..."
        
        if ! command -v snap >/dev/null 2>&1; then
            run_with_animated_spinner "Установка snapd" \
                sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get update -qq && apt-get install -y --no-install-recommends snapd" || {
                log_error "Не удалось установить snapd"
                exit 1
            }
        fi
        
        run_with_animated_spinner "Установка Certbot через snap" \
            sudo bash -c "snap install core 2>/dev/null; snap refresh core 2>/dev/null; snap install --classic certbot 2>/dev/null; ln -sf /snap/bin/certbot /usr/bin/certbot" || {
            log_error "Не удалось установить Certbot"
            exit 1
        }
    }
    
    if certbot plugins 2>/dev/null | grep -qi 'nginx'; then
        log_success "Плагин Certbot Nginx успешно настроен"
    else
        log_error "Не удалось установить плагин Nginx для Certbot"
        exit 1
    fi
}

configure_nginx() {
    local domain="$1"
    local port="$2"
    
    log_info "Конфигурация Nginx для $domain:$port"
    
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
        sudo ln -s "$NGINX_CONF" "$NGINX_LINK" 2>/dev/null || true
    fi

    run_with_spinner "Проверка конфигурации Nginx" sudo nginx -t || {
        log_error "Некорректная конфигурация Nginx"
        exit 1
    }
    
    run_with_spinner "Перезагрузка Nginx" sudo systemctl reload nginx || {
        log_error "Не удалось перезагрузить Nginx"
        exit 1
    }
}

show_docker_images() {
    if command -v docker >/dev/null 2>&1 && [[ -f "docker-compose.yml" ]]; then
        echo -e "${BLUE}${BOLD}[INFO]${NC} Docker контейнеры в проекте:"
        docker-compose config --services 2>/dev/null | sed 's/^/  - /' || true
    fi
}

cleanup_old_installation() {
    log_warn "Обнаружена остаток старой установки (конфигурация найдена, каталог удалён)."
    echo ""
    
    REPLY=""
    while true; do
        log_input "Выберите действие [Y]es/[N]o/[M]anual (default: Y): "
        read -r -n1 REPLY < /dev/tty || true
        echo ""
        
        if [[ -z "$REPLY" ]]; then
            REPLY="y"
        fi
        
        case "${REPLY,,}" in
            y) 
                log_info "Удаление старой конфигурации для чистой переустановки..."
                run_with_spinner "Удаление символической ссылки" sudo rm -f "$NGINX_LINK" || true
                run_with_spinner "Удаление конфигурации Nginx" sudo rm -f "$NGINX_CONF" || true
                
                if [[ ! -f "$NGINX_CONF" ]] && [[ ! -L "$NGINX_LINK" ]]; then
                    log_success "Старая конфигурация удалена. Переходим к новой установке."
                    echo ""
                    return 0
                else
                    log_error "Не удалось удалить конфигурацию. Попробуйте ручное удаление."
                    manual_cleanup
                    return 0
                fi
                ;;
            n) 
                log_error "Установка отменена пользователем."
                exit 1
                ;;
            m) 
                manual_cleanup
                return 0
                ;;
            *) 
                log_warn "Введите Y (автоматически), N (отмена) или M (ручное удаление)"
                ;;
        esac
    done
}

manual_cleanup() {
    echo ""
    log_warn "Переходим в режим ручного удаления конфигурации."
    echo ""
    log_info "Выполните следующие команды:"
    echo -e "  ${CYAN}sudo rm -f $NGINX_LINK${NC}"
    echo -e "  ${CYAN}sudo rm -f $NGINX_CONF${NC}"
    echo ""
    
    REPLY=""
    while true; do
        log_input "Вы удалили конфигурацию вручную? (Y/n): "
        read -r -n1 REPLY < /dev/tty || true
        echo ""
        
        if [[ -z "$REPLY" ]]; then
            REPLY="y"
        fi
        
        case "${REPLY,,}" in
            y)
                if [[ ! -f "$NGINX_CONF" ]] && [[ ! -L "$NGINX_LINK" ]]; then
                    log_success "Конфигурация удалена успешно. Переходим к новой установке."
                    echo ""
                    return 0
                else
                    log_error "Конфигурация всё ещё существует:"
                    [[ -f "$NGINX_CONF" ]] && echo -e "  ${CYAN}$NGINX_CONF${NC}"
                    [[ -L "$NGINX_LINK" ]] && echo -e "  ${CYAN}$NGINX_LINK${NC}"
                    log_warn "Пожалуйста, удалите эти файлы и попробуйте снова."
                    exit 1
                fi
                ;;
            n)
                log_error "Конфигурация не удалена. Установка отменена."
                exit 1
                ;;
            *)
                log_warn "Введите Y (да) или N (нет)"
                ;;
        esac
    done
}

show_header
ensure_sudo_refresh

if [[ -f "$NGINX_CONF" ]]; then
    if [[ -d "$PROJECT_DIR" ]]; then
        log_info "Обнаружена существующая конфигурация."
        
        cd "$PROJECT_DIR"
        
        DOMAIN=$(get_domain_from_nginx)
        YOOKASSA_PORT=$(get_port_from_nginx)
        
        run_with_animated_spinner "Принудительное обновление из Git" bash -c "git fetch --all && git reset --hard origin/main && git clean -fd" || {
            log_error "Не удалось принудительно обновить репозиторий"
            exit 1
        }
        
        echo ""
        show_docker_images
        echo ""
        
        run_with_animated_spinner "Пересборка контейнеров" \
            sudo bash -c "docker-compose down --remove-orphans && docker-compose up -d --build" || {
            log_error "Не удалось пересобрать контейнеры"
            exit 1
        }
        
        echo ""
        echo -e "${GREEN}┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓${NC}"
        echo -e "${GREEN}┃                  ОБНОВЛЕНИЕ ЗАВЕРШЕНО!                       ┃${NC}"
        echo -e "${GREEN}┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛${NC}"
        echo ""
        echo -e " ${BOLD}Адрес панели:${NC}     https://${DOMAIN}:${YOOKASSA_PORT}/login"
        echo -e " ${BOLD}Данные входа:${NC}     ${CYAN}admin${NC} / ${CYAN}admin${NC}"
        echo -e " ${BOLD}Webhook URL:${NC}      https://${DOMAIN}:${YOOKASSA_PORT}/yookassa-webhook"
        echo ""
        echo -e " ${BOLD}SSL Сертификаты:${NC}"
        echo -e "   Публичный:  ${YELLOW}/etc/letsencrypt/live/${DOMAIN}/fullchain.pem${NC}"
        echo -e "   Приватный:  ${YELLOW}/etc/letsencrypt/live/${DOMAIN}/privkey.pem${NC}"
        echo ""
        show_footer
        exit 0
    else
        cleanup_old_installation
    fi
fi

log_info "Инициализация процесса установки..."
echo ""

ensure_packages
ensure_services
ensure_certbot_nginx

if [[ ! -d "$PROJECT_DIR/.git" ]]; then
    run_with_animated_spinner "Клонирование репозитория" git clone "$REPO_URL" "$PROJECT_DIR" || {
        log_error "Не удалось клонировать репозиторий"
        exit 1
    }
else
    log_warn "Каталог проекта уже существует, пропускаем клонирование"
fi
cd "$PROJECT_DIR"

echo ""

DOMAIN=""
while [[ -z "$DOMAIN" ]]; do
    log_input "Введите ваш домен (без http/s): "
    read -r USER_DOMAIN_INPUT < /dev/tty || true
    
    if [[ -z "$USER_DOMAIN_INPUT" ]]; then
        log_warn "Домен не может быть пустым."
        continue
    fi
    
    DOMAIN=$(sanitize_domain "$USER_DOMAIN_INPUT")
    if [[ -z "$DOMAIN" ]]; then
        log_warn "Домен содержит недопустимые символы."
        DOMAIN=""
    fi
done

EMAIL=""
while [[ -z "$EMAIL" ]]; do
    log_input "Введите Email для SSL (Let's Encrypt): "
    read -r EMAIL < /dev/tty || true
    
    if [[ -z "$EMAIL" ]]; then
        log_warn "Email не может быть пустым."
    fi
done

echo ""

SERVER_IP=$(get_server_ip || true)
DOMAIN_IP=$(resolve_domain_ip "$DOMAIN" || true)

if [[ -n "$SERVER_IP" ]]; then
    log_success "IP сервера: $SERVER_IP"
fi

if [[ -n "$DOMAIN_IP" ]]; then
    log_success "IP домена: $DOMAIN_IP"
fi

if [[ -n "$SERVER_IP" ]] && [[ -n "$DOMAIN_IP" ]] && [[ "$SERVER_IP" != "$DOMAIN_IP" ]]; then
    log_warn "Внимание: IP сервера ($SERVER_IP) отличается от IP домена ($DOMAIN_IP)."
    log_warn "Это может привести к ошибке выдачи SSL сертификата."
    
    REPLY=""
    while true; do
        log_input "Продолжить всё равно? (y/n): "
        read -r -n1 REPLY < /dev/tty || true
        echo ""
        case "$REPLY" in
            [yY]) break ;;
            [nN]) log_error "Установка отменена."; exit 1 ;;
            *) ;;
        esac
    done
fi

echo ""

if command -v ufw >/dev/null 2>&1 && sudo ufw status 2>/dev/null | grep -q 'Status: active'; then
    run_with_spinner "Открытие портов UFW (80, 443, 1488, 8443)" \
        sudo bash -c "ufw allow 80/tcp && ufw allow 443/tcp && ufw allow 1488/tcp && ufw allow 8443/tcp" || {
        log_warn "Не удалось настроить UFW"
    }
fi

echo ""

if [[ -d "/etc/letsencrypt/live/${DOMAIN}" ]]; then
    log_success "SSL сертификаты уже существуют для $DOMAIN"
else
    log_info "Выпуск SSL сертификата от Let's Encrypt для $DOMAIN..."
    
    run_with_animated_spinner "Получение сертификата (может занять время)" \
        sudo bash -c "certbot --nginx -d $DOMAIN --email $EMAIL --agree-tos --non-interactive --redirect --no-eff-email 2>&1" || {
        log_error "Не удалось получить SSL сертификат. Проверьте настройки DNS."
        exit 1
    }
fi

echo ""
log_input "Порт для вебхуков YooKassa [443/8443] (default: 8443): "
read -r YOOKASSA_PORT_INPUT < /dev/tty || true
YOOKASSA_PORT="${YOOKASSA_PORT_INPUT:-8443}"

if [[ "$YOOKASSA_PORT" != "443" && "$YOOKASSA_PORT" != "8443" ]]; then
    log_warn "Неподдерживаемый порт, используется 8443"
    YOOKASSA_PORT=8443
fi

echo ""
configure_nginx "$DOMAIN" "$YOOKASSA_PORT"

echo ""
show_docker_images
echo ""

run_with_animated_spinner "Сборка и запуск Docker контейнеров" \
    sudo bash -c "if [ -n \"\$(docker-compose ps -q 2>/dev/null)\" ]; then docker-compose down --remove-orphans; fi; docker-compose up -d --build" || {
    log_error "Не удалось запустить Docker контейнеры"
    exit 1
}

echo ""
echo -e "${GREEN}┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓${NC}"
echo -e "${GREEN}┃                  УСТАНОВКА ЗАВЕРШЕНА!                        ┃${NC}"
echo -e "${GREEN}┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛${NC}"
echo ""
echo -e " ${BOLD}Адрес панели:${NC}     https://${DOMAIN}:${YOOKASSA_PORT}/login"
echo -e " ${BOLD}Данные входа:${NC}     ${CYAN}admin${NC} / ${CYAN}admin${NC}"
echo -e " ${BOLD}Webhook URL:${NC}      https://${DOMAIN}:${YOOKASSA_PORT}/yookassa-webhook"
echo ""
echo -e " ${BOLD}SSL Сертификаты:${NC}"
echo -e "   Публичный:  ${YELLOW}/etc/letsencrypt/live/${DOMAIN}/fullchain.pem${NC}"
echo -e "   Приватный:  ${YELLOW}/etc/letsencrypt/live/${DOMAIN}/privkey.pem${NC}"
echo ""
echo -e "${YELLOW} ⚠  Пожалуйста, смените пароль сразу после входа!${NC}"
echo ""
show_footer
