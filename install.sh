#!/usr/bin/env bash
set -u
set -o pipefail

APP_NAME="Digital_Footprint_System"
APP_ROOT="/opt/${APP_NAME}"
SCRIPTS_DST="${APP_ROOT}/scripts"
VENV_DIR="${APP_ROOT}/venv"
RUNNER_SCRIPT="${APP_ROOT}/run_pipeline.sh"
SYSTEMD_SERVICE="/etc/systemd/system/digital-footprint.service"
SYSTEMD_TIMER="/etc/systemd/system/digital-footprint.timer"
LOG_DIR="/var/log/digital_footprint"
REPO_MIRROR_BASE="https://mirror.yandex.ru/mirrors/elastic/9/pool/main"

# Если install.sh лежит рядом с папкой scripts
INSTALL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_SRC="${INSTALL_DIR}/scripts"

ELASTIC_PASSWORD="qwerty"
ELASTIC_USER="elastic"

log() {
    echo "[$(date '+%F %T')] $*"
}

fail() {
    echo "[$(date '+%F %T')] ERROR: $*" >&2
    exit 1
}

require_root() {
    if [[ "${EUID}" -ne 0 ]]; then
        fail "Скрипт нужно запускать от root"
    fi
}

detect_pkg_manager() {
    if command -v apt-get >/dev/null 2>&1; then
        PKG_MANAGER="apt"
    else
        fail "Поддерживается только Debian/Ubuntu с apt"
    fi
}

install_base_packages() {
    log "Установка базовых пакетов"
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        curl \
        wget \
        ca-certificates \
        gnupg \
        apt-transport-https \
        software-properties-common \
        unzip \
        tar \
        rsync \
        systemd \
        openjdk-17-jre-headless \
        python3 \
        python3-pip \
        python3-venv
}

detect_arch() {
    local arch
    arch="$(dpkg --print-architecture)"
    case "${arch}" in
        amd64) ELASTIC_ARCH="amd64" ;;
        arm64) ELASTIC_ARCH="arm64" ;;
        *)
            fail "еподдерживаемая архитектура: ${arch}"
            ;;
    esac
}

find_latest_deb() {
    local package_name="$1"
    local letter="$2"
    local page
    local relpath

    page="$(curl -fsSL "${REPO_MIRROR_BASE}/${letter}/" )" || return 1
    relpath="$(echo "${page}" | grep -oE "${package_name}_[^\"]+_${ELASTIC_ARCH}\.deb" | sort -V | tail -n1)" || true

    if [[ -z "${relpath}" ]]; then
        return 1
    fi

    echo "${REPO_MIRROR_BASE}/${letter}/${package_name}/${relpath}"
}

download_and_install_elastic_deb() {
    local pkg="$1"
    local letter="$2"
    local url
    local tmpfile

    url="$(find_latest_deb "${pkg}" "${letter}")" || fail "Не удалось найти пакет ${pkg} в зеркале"
    tmpfile="/tmp/${pkg}.deb"

    log "Скачивание ${pkg}: ${url}"
    curl -fL "${url}" -o "${tmpfile}" || fail "Не удалось скачать ${pkg}"

    log "Установка ${pkg}"
    dpkg -i "${tmpfile}" || apt-get -f install -y
}

install_elk_from_yandex_mirror() {
    detect_arch

    log "Установка Elasticsearch, Logstash, Kibana из зеркала Яндекса"
    download_and_install_elastic_deb "elasticsearch" "e"
    download_and_install_elastic_deb "logstash" "l"
    download_and_install_elastic_deb "kibana" "k"
}

prepare_directories() {
    log "Создание структуры каталогов"

    mkdir -p "${APP_ROOT}"
    mkdir -p "${SCRIPTS_DST}"
    mkdir -p "${APP_ROOT}/variables/tokens"
    mkdir -p "${APP_ROOT}/variables"
    mkdir -p "${APP_ROOT}/result"
    mkdir -p "${LOG_DIR}"

    # Базовые result-структуры под приложенные скрипты
    mkdir -p "${APP_ROOT}/result/domains-monitor/"{new,old,logs,jsonl}
    mkdir -p "${APP_ROOT}/result/github-monitor/"{new,old,logs,jsonl}
    mkdir -p "${APP_ROOT}/result/hh/"{new,old,logs,jsonl}
    mkdir -p "${APP_ROOT}/result/infosearch/"{new,old,logs,jsonl}
    mkdir -p "${APP_ROOT}/result/leakcheck/"{new,old,logs,jsonl}
    mkdir -p "${APP_ROOT}/result/google_cse/"{new,old,logs,jsonl}

    # Дополнительные директории на случай расширения
    mkdir -p "${APP_ROOT}/scripts/collect"
    mkdir -p "${APP_ROOT}/scripts/converters"
    mkdir -p "${APP_ROOT}/scripts/logstash_conf"
}

copy_project_files() {
    [[ -d "${SCRIPTS_SRC}" ]] || fail "Не найдена папка scripts рядом с install.sh: ${SCRIPTS_SRC}"

    log "Копирование scripts -> ${SCRIPTS_DST}"
    rsync -a --delete "${SCRIPTS_SRC}/" "${SCRIPTS_DST}/"

    if [[ -f "${SCRIPTS_DST}/logstash_conf/pipelines.yml" ]]; then
        log "Копирование pipelines.yml -> /etc/logstash/pipelines.yml"
        cp -f "${SCRIPTS_DST}/logstash_conf/pipelines.yml" /etc/logstash/pipelines.yml
    else
        log "Файл ${SCRIPTS_DST}/logstash_conf/pipelines.yml не найден, пропускаю"
    fi
}

generate_requirements() {
    local req_file="${APP_ROOT}/requirements.txt"

    log "Формирование requirements.txt"
    cat > "${req_file}" <<'EOF'
requests
python-whois
pyOpenSSL
shodan
google-api-python-client
leakcheck
EOF
}

install_python_dependencies() {
    log "Создание Python virtualenv"
    python3 -m venv "${VENV_DIR}" || fail "Не удалось создать venv"

    log "Обновление pip/setuptools/wheel"
    "${VENV_DIR}/bin/pip" install --upgrade pip setuptools wheel || fail "Не удалось обновить pip"

    generate_requirements

    log "Установка Python-зависимостей"
    "${VENV_DIR}/bin/pip" install -r "${APP_ROOT}/requirements.txt" || fail "Не удалось установить Python зависимости"
}

create_runner_script() {
    log "Создание orchestrator script: ${RUNNER_SCRIPT}"

    cat > "${RUNNER_SCRIPT}" <<'EOF'
#!/usr/bin/env bash
set -u
set -o pipefail

APP_ROOT="/opt/Digital_Footprint_System"
SCRIPTS_DIR="${APP_ROOT}/scripts"
COLLECT_DIR="${SCRIPTS_DIR}/collect"
CONVERTERS_DIR="${SCRIPTS_DIR}/converters"
VENV_PYTHON="${APP_ROOT}/venv/bin/python"
MAIN_LOG_DIR="/var/log/digital_footprint"
RUN_LOG="${MAIN_LOG_DIR}/pipeline.log"

mkdir -p "${MAIN_LOG_DIR}"

log() {
    echo "[$(date '+%F %T')] $*" | tee -a "${RUN_LOG}"
}

run_python_dir() {
    local dir="$1"
    local stage="$2"

    if [[ ! -d "${dir}" ]]; then
        log "Каталог ${dir} не найден, этап ${stage} пропущен"
        return 0
    fi

    mapfile -t files < <(find "${dir}" -maxdepth 1 -type f -name "*.py" | sort)

    if [[ "${#files[@]}" -eq 0 ]]; then
        log "В каталоге ${dir} нет *.py файлов, этап ${stage} пропущен"
        return 0
    fi

    log "Этап ${stage}: найдено ${#files[@]} скриптов"

    local f
    for f in "${files[@]}"; do
        log "Запуск ${stage}: ${f}"
        if "${VENV_PYTHON}" "${f}" >> "${RUN_LOG}" 2>&1; then
            log "OK ${stage}: ${f}"
        else
            rc=$?
            log "FAIL ${stage}: ${f}, exit_code=${rc}"
        fi
    done
}

main() {
    log "==== START daily pipeline ===="
    run_python_dir "${COLLECT_DIR}" "collect"
    run_python_dir "${CONVERTERS_DIR}" "converters"
    log "==== END daily pipeline ===="
}

main "$@"
EOF

    chmod +x "${RUNNER_SCRIPT}"
}

create_systemd_units() {
    log "Создание systemd service"
    cat > "${SYSTEMD_SERVICE}" <<EOF
[Unit]
Description=Digital Footprint daily pipeline
After=network-online.target elasticsearch.service logstash.service
Wants=network-online.target

[Service]
Type=oneshot
User=root
WorkingDirectory=${APP_ROOT}
ExecStart=${RUNNER_SCRIPT}
StandardOutput=append:${LOG_DIR}/systemd_service.log
StandardError=append:${LOG_DIR}/systemd_service.log

[Install]
WantedBy=multi-user.target
EOF

    log "Создание systemd timer"
    cat > "${SYSTEMD_TIMER}" <<'EOF'
[Unit]
Description=Run Digital Footprint daily pipeline at 18:00

[Timer]
OnCalendar=*-*-* 18:00:00
Persistent=true
Unit=digital-footprint.service

[Install]
WantedBy=timers.target
EOF

    systemctl daemon-reload
    systemctl enable digital-footprint.timer
    systemctl start digital-footprint.timer
}

configure_elasticsearch() {
    log "Базовая настройка Elasticsearch"

    mkdir -p /etc/elasticsearch
    cp -n /etc/elasticsearch/elasticsearch.yml /etc/elasticsearch/elasticsearch.yml.bak 2>/dev/null || true

    if ! grep -q "^xpack.security.enabled:" /etc/elasticsearch/elasticsearch.yml 2>/dev/null; then
        cat >> /etc/elasticsearch/elasticsearch.yml <<'EOF'

xpack.security.enabled: true
discovery.type: single-node
network.host: 127.0.0.1
http.port: 9200
EOF
    fi

    systemctl enable elasticsearch
    systemctl restart elasticsearch

    log "Ожидание запуска Elasticsearch"
    local i
    for i in $(seq 1 60); do
        if curl -sk --connect-timeout 2 https://127.0.0.1:9200 >/dev/null 2>&1 || \
           curl -s --connect-timeout 2 http://127.0.0.1:9200 >/dev/null 2>&1; then
            log "Elasticsearch доступен"
            break
        fi
        sleep 5
    done
}

try_set_elastic_password() {
    log "Попытка установить пароль для пользователя ${ELASTIC_USER}"

    if command -v /usr/share/elasticsearch/bin/elasticsearch-reset-password >/dev/null 2>&1; then
        if /usr/share/elasticsearch/bin/elasticsearch-reset-password -u "${ELASTIC_USER}" -b -s <<EOF
${ELASTIC_PASSWORD}
${ELASTIC_PASSWORD}
EOF
        then
            log "Пароль пользователя ${ELASTIC_USER} установлен через elasticsearch-reset-password"
            return 0
        fi
    fi

    if command -v /usr/share/elasticsearch/bin/elasticsearch-setup-passwords >/dev/null 2>&1; then
        log "Найдена elasticsearch-setup-passwords, но полностью безынтерактивный режим может зависеть от версии"
    fi

    log "Не удалось гарантированно автоматически установить пароль. Проверь вручную при необходимости."
    return 0
}

configure_kibana() {
    log "Базовая настройка Kibana"
    cp -n /etc/kibana/kibana.yml /etc/kibana/kibana.yml.bak 2>/dev/null || true

    if ! grep -q "^server.host:" /etc/kibana/kibana.yml 2>/dev/null; then
        cat >> /etc/kibana/kibana.yml <<'EOF'

server.host: "0.0.0.0"
EOF
    fi

    systemctl enable kibana
    systemctl restart kibana || true
}

configure_logstash() {
    log "Включение Logstash"
    systemctl enable logstash
    systemctl restart logstash || true
}

create_env_examples() {
    log "Создание шаблонов переменных"

    touch "${APP_ROOT}/variables/domains"
    touch "${APP_ROOT}/variables/zone"
    touch "${APP_ROOT}/variables/github_queries"

    touch "${APP_ROOT}/variables/tokens/domains_monitor"
    touch "${APP_ROOT}/variables/tokens/shodan"
    touch "${APP_ROOT}/variables/tokens/github"
    touch "${APP_ROOT}/variables/tokens/hh"
    touch "${APP_ROOT}/variables/tokens/hh_user_agent"
    touch "${APP_ROOT}/variables/tokens/infosearch"
    touch "${APP_ROOT}/variables/tokens/leakcheck"
    touch "${APP_ROOT}/variables/tokens/google_api_key"

    chmod -R 700 "${APP_ROOT}/variables"
}

main() {
    require_root
    detect_pkg_manager
    install_base_packages
    install_elk_from_yandex_mirror
    prepare_directories
    copy_project_files
    install_python_dependencies
    create_runner_script
    create_env_examples
    configure_elasticsearch
    try_set_elastic_password
    configure_logstash
    configure_kibana
    create_systemd_units

    log "Установка завершена"
    log "Папка приложения: ${APP_ROOT}"
    log "Сервис: digital-footprint.service"
    log "Таймер: digital-footprint.timer"
    log "Проверка таймера: systemctl list-timers --all | grep digital-footprint"
    log "Ручной запуск: systemctl start digital-footprint.service"
}

main "$@"
