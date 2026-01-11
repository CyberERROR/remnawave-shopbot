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
PROJECT_DIR="remnawave-shopbot"
NGINX_CONF="/etc/nginx/sites-available/${PROJECT_DIR}.conf"
NGINX_LINK="/etc/nginx/sites-enabled/${PROJECT_DIR}.conf"

# Переменные удаления
REMOVE_CONFIG=false
REMOVE_DOCKER=false
REMOVE_PROJECT=false
REMOVE_CERTBOT=false
REMOVE_SSL=false
REMOVE_ALL=false

# --- Функции отображения ---

show_header() {
    clear
    echo -e "${PURPLE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${PURPLE}${BOLD}        REMNAWAVE SHOPBOT UNINSTALLER (Удаление)             ${NC}"
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

# Функция спиннера с информацией о процессе
run_with_spinner() {
    local message="$1"
    shift
    local cmd=("$@")

    echo -ne "${BLUE}${BOLD}[➜]${NC} ${message}... "

    local temp_log
    temp_log=$(mktemp)
    
    # Запускаем команду
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

# --- Основные функции удаления ---

ensure_sudo_refresh() {
    if ! sudo -v; then
        log_error "Требуются права sudo. Введите пароль выше."
        exit 1
    fi
}

show_menu() {
    echo ""
    echo -e "${YELLOW}${BOLD}Что вы хотите удалить?${NC}"
    echo ""
    echo -e " ${CYAN}1)${NC} Конфигурация Nginx и сертификаты (${CYAN}быстрое удаление${NC})"
    echo -e " ${CYAN}2)${NC} Полное удаление приложения (конфиг + проект + контейнеры)"
    echo -e " ${CYAN}3)${NC} Максимальное удаление (всё + Certbot + зависимости)"
    echo -e " ${CYAN}4)${NC} Настроить вручную"
    echo -e " ${CYAN}0)${NC} Отмена"
    echo ""
    log_input "Выберите вариант [0-4] (default: 0): "
    read -r -n1 CHOICE < /dev/tty || true
    echo ""
    
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
            REMOVE_DOCKER=true
            REMOVE_PROJECT=true
            REMOVE_SSL=true
            ;;
        3)
            REMOVE_ALL=true
            REMOVE_CONFIG=true
            REMOVE_DOCKER=true
            REMOVE_PROJECT=true
            REMOVE_SSL=true
            REMOVE_CERTBOT=true
            ;;
        4)
            show_custom_menu
            ;;
        0)
            log_error "Удаление отменено пользователем."
            exit 0
            ;;
        *)
            log_warn "Неверный выбор. Отмена."
            exit 1
            ;;
    esac
}

show_custom_menu() {
    echo ""
    echo -e "${YELLOW}${BOLD}Дополнительные опции удаления:${NC}"
    echo ""
    
    # Конфиг Nginx
    if [[ -f "$NGINX_CONF" ]] || [[ -L "$NGINX_LINK" ]]; then
        log_input "Удалить конфигурацию Nginx? (Y/n): "
        read -r -n1 REPLY < /dev/tty || true
        echo ""
        if [[ -z "$REPLY" ]] || [[ "$REPLY" =~ [yY] ]]; then
            REMOVE_CONFIG=true
        fi
    fi
    
    # Сертификаты SSL
    DOMAIN=$(get_domain_from_nginx || echo "")
    if [[ -n "$DOMAIN" ]] && [[ -d "/etc/letsencrypt/live/${DOMAIN}" ]]; then
        log_input "Удалить SSL сертификаты для $DOMAIN? (Y/n): "
        read -r -n1 REPLY < /dev/tty || true
        echo ""
        if [[ -z "$REPLY" ]] || [[ "$REPLY" =~ [yY] ]]; then
            REMOVE_SSL=true
        fi
    fi
    
    # Docker контейнеры
    if command -v docker >/dev/null 2>&1 && docker ps -a 2>/dev/null | grep -q "$PROJECT_DIR"; then
        log_input "Остановить и удалить Docker контейнеры? (Y/n): "
        read -r -n1 REPLY < /dev/tty || true
        echo ""
        if [[ -z "$REPLY" ]] || [[ "$REPLY" =~ [yY] ]]; then
            REMOVE_DOCKER=true
        fi
    fi
    
    # Каталог проекта
    if [[ -d "$PROJECT_DIR" ]]; then
        log_input "Удалить каталог проекта ($PROJECT_DIR)? (y/N): "
        read -r -n1 REPLY < /dev/tty || true
        echo ""
        if [[ "$REPLY" =~ [yY] ]]; then
            REMOVE_PROJECT=true
        fi
    fi
    
    # Certbot
    if command -v certbot >/dev/null 2>&1; then
        log_input "Удалить Certbot? (y/N): "
        read -r -n1 REPLY < /dev/tty || true
        echo ""
        if [[ "$REPLY" =~ [yY] ]]; then
            REMOVE_CERTBOT=true
        fi
    fi
}

get_domain_from_nginx() {
    if [[ -f "$NGINX_CONF" ]]; then
        grep "server_name" "$NGINX_CONF" | awk '{print $2}' | sed 's/;//' | head -n1
    fi
}

show_removal_summary() {
    echo ""
    echo -e "${YELLOW}${BOLD}━━━━ Итоговый список удаления ━━━━${NC}"
    echo ""
    
    [[ "$REMOVE_CONFIG" == "true" ]] && echo -e " ${RED}✘${NC} Конфигурация Nginx"
    [[ "$REMOVE_SSL" == "true" ]] && echo -e " ${RED}✘${NC} SSL сертификаты"
    [[ "$REMOVE_DOCKER" == "true" ]] && echo -e " ${RED}✘${NC} Docker контейнеры и образы"
    [[ "$REMOVE_PROJECT" == "true" ]] && echo -e " ${RED}✘${NC} Каталог проекта ($PROJECT_DIR)"
    [[ "$REMOVE_CERTBOT" == "true" ]] && echo -e " ${RED}✘${NC} Certbot"
    
    echo ""
    log_input "Вы уверены? Это действие невозможно отменить! (y/N): "
    read -r -n1 CONFIRM < /dev/tty || true
    echo ""
    
    if [[ ! "$CONFIRM" =~ [yY] ]]; then
        log_warn "Удаление отменено."
        exit 0
    fi
}

remove_nginx_config() {
    if [[ -f "$NGINX_CONF" ]] || [[ -L "$NGINX_LINK" ]]; then
        log_info "Удаление конфигурации Nginx..."
        run_with_spinner "Удаление символической ссылки" sudo rm -f "$NGINX_LINK" || true
        run_with_spinner "Удаление конфигурации" sudo rm -f "$NGINX_CONF" || true
        
        if command -v nginx >/dev/null 2>&1; then
            run_with_spinner "Проверка конфигурации Nginx" sudo nginx -t || log_warn "Проверка конфигурации завершилась с ошибкой"
            run_with_spinner "Перезагрузка Nginx" sudo systemctl reload nginx || log_warn "Не удалось перезагрузить Nginx"
        fi
        
        log_success "Конфигурация Nginx удалена"
    else
        log_warn "Конфигурация Nginx не найдена"
    fi
}

remove_ssl_certs() {
    if [[ "$REMOVE_SSL" == "true" ]]; then
        local domain
        domain=$(get_domain_from_nginx || echo "")
        
        if [[ -z "$domain" ]]; then
            log_warn "Не удалось определить домен из конфигурации"
            return 1
        fi
        
        if [[ -d "/etc/letsencrypt/live/${domain}" ]]; then
            log_info "Удаление SSL сертификатов для $domain..."
            run_with_spinner "Удаление директории сертификатов" sudo rm -rf "/etc/letsencrypt/live/${domain}" || true
            run_with_spinner "Удаление архива сертификатов" sudo rm -rf "/etc/letsencrypt/archive/${domain}" || true
            run_with_spinner "Удаление renewal конфигурации" sudo rm -f "/etc/letsencrypt/renewal/${domain}.conf" || true
            log_success "SSL сертификаты удалены"
        else
            log_warn "SSL сертификаты для $domain не найдены"
        fi
    fi
}

remove_docker_containers() {
    if [[ "$REMOVE_DOCKER" == "true" ]]; then
        if ! command -v docker >/dev/null 2>&1; then
            log_warn "Docker не установлен"
            return 0
        fi
        
        log_info "Удаление Docker контейнеров..."
        
        # Переходим в каталог проекта если он существует
        if [[ -d "$PROJECT_DIR" ]]; then
            cd "$PROJECT_DIR"
            
            if [[ -f "docker-compose.yml" ]]; then
                run_with_spinner "Остановка и удаление контейнеров (docker-compose)" \
                    sudo bash -c "docker-compose down --rmi all --remove-orphans 2>/dev/null" || true
            fi
            
            cd - > /dev/null
        fi
        
        # Удаляем все контейнеры относящиеся к проекту
        local containers
        containers=$(docker ps -a --format "{{.Names}}" 2>/dev/null | grep "$PROJECT_DIR" || echo "")
        
        if [[ -n "$containers" ]]; then
            log_warn "Удаление оставшихся контейнеров..."
            echo "$containers" | while read -r container; do
                run_with_spinner "Удаление контейнера $container" sudo docker rm -f "$container" || true
            done
        fi
        
        log_success "Docker контейнеры удалены"
    fi
}

remove_project_directory() {
    if [[ "$REMOVE_PROJECT" == "true" ]]; then
        if [[ -d "$PROJECT_DIR" ]]; then
            log_info "Удаление каталога проекта..."
            run_with_spinner "Удаление $PROJECT_DIR" sudo rm -rf "$PROJECT_DIR" || true
            log_success "Каталог проекта удалён"
        else
            log_warn "Каталог проекта не найден"
        fi
    fi
}

remove_certbot() {
    if [[ "$REMOVE_CERTBOT" == "true" ]]; then
        if ! command -v certbot >/dev/null 2>&1; then
            log_warn "Certbot не установлен"
            return 0
        fi
        
        log_info "Удаление Certbot..."
        
        # Проверяем как был установлен Certbot
        if snap list 2>/dev/null | grep -q certbot; then
            log_warn "Certbot установлен через snap, удаление..."
            run_with_spinner "Удаление Certbot (snap)" sudo snap remove certbot || true
        fi
        
        if dpkg -l 2>/dev/null | grep -q certbot; then
            log_warn "Certbot установлен через apt, удаление..."
            run_with_spinner "Удаление python3-certbot-nginx" \
                sudo bash -c "export DEBIAN_FRONTEND=noninteractive && apt-get remove -y python3-certbot-nginx certbot 2>/dev/null" || true
        fi
        
        # Удаляем директорию Let's Encrypt (если выбран флаг)
        if [[ -d "/etc/letsencrypt" ]]; then
            log_input "Удалить все сертификаты Let's Encrypt (/etc/letsencrypt)? (y/N): "
            read -r -n1 REPLY < /dev/tty || true
            echo ""
            if [[ "$REPLY" =~ [yY] ]]; then
                run_with_spinner "Удаление /etc/letsencrypt" sudo rm -rf /etc/letsencrypt || true
                log_warn "Все сертификаты Let's Encrypt удалены"
            fi
        fi
        
        log_success "Certbot удалён"
    fi
}

# --- Начало выполнения ---

show_header
ensure_sudo_refresh

# Проверяем что это не главный каталог
if [[ "$PWD" == "/" ]] || [[ "$PWD" == "/root" ]] || [[ "$PWD" == "$HOME" ]]; then
    log_warn "Не рекомендуется запускать скрипт из корневого или домашнего каталога"
fi

# Проверяем есть ли что удалять
if ! [[ -f "$NGINX_CONF" ]] && ! [[ -L "$NGINX_LINK" ]] && ! [[ -d "$PROJECT_DIR" ]]; then
    log_error "Remnawave Shopbot не установлен. Нечего удалять."
    exit 1
fi

log_info "Обнаружена существующая установка Remnawave Shopbot"
echo ""

# Показываем меню
show_menu

# Показываем итоговый список
show_removal_summary

echo ""
echo -e "${RED}${BOLD}════════════════════════════════════════════════════════════════${NC}"
echo -e "${RED}${BOLD}               НАЧИНАЕТСЯ УДАЛЕНИЕ...                           ${NC}"
echo -e "${RED}${BOLD}════════════════════════════════════════════════════════════════${NC}"
echo ""

# Выполняем удаление
[[ "$REMOVE_CONFIG" == "true" ]] && remove_nginx_config
[[ "$REMOVE_SSL" == "true" ]] && remove_ssl_certs
[[ "$REMOVE_DOCKER" == "true" ]] && remove_docker_containers
[[ "$REMOVE_PROJECT" == "true" ]] && remove_project_directory
[[ "$REMOVE_CERTBOT" == "true" ]] && remove_certbot

echo ""
echo -e "${GREEN}┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓${NC}"
echo -e "${GREEN}┃                  УДАЛЕНИЕ ЗАВЕРШЕНО!                          ┃${NC}"
echo -e "${GREEN}┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛${NC}"
echo ""

# Показываем оставшиеся файлы/сервисы
if [[ -f "$NGINX_CONF" ]] || [[ -L "$NGINX_LINK" ]]; then
    log_warn "Конфигурация Nginx всё ещё существует:"
    [[ -f "$NGINX_CONF" ]] && echo -e "  ${CYAN}$NGINX_CONF${NC}"
    [[ -L "$NGINX_LINK" ]] && echo -e "  ${CYAN}$NGINX_LINK${NC}"
fi

if [[ -d "$PROJECT_DIR" ]]; then
    log_warn "Каталог проекта всё ещё существует: ${CYAN}$PROJECT_DIR${NC}"
fi

if command -v docker >/dev/null 2>&1 && docker ps -a 2>/dev/null | grep -q "$PROJECT_DIR"; then
    log_warn "Docker контейнеры проекта всё ещё существуют"
fi

echo ""
log_info "Для полного удаления остатков выполните вручную:"
echo -e "  ${CYAN}sudo rm -rf $PROJECT_DIR${NC}"
echo -e "  ${CYAN}sudo rm -f $NGINX_LINK $NGINX_CONF${NC}"
echo ""

show_footer
