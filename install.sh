#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://github.com/NikitosLeo/Dissertation.git"
APP_DIR="/opt/Digital_Footprint_System"
REPO_DIR="$APP_DIR/repo"
SCRIPTS_SRC="$REPO_DIR/scripts"
SCRIPTS_DST="$APP_DIR/scripts"
VENV_DIR="$APP_DIR/venv"
TMP_DIR="/tmp/dfs-install"

ES_DEB_URL="https://mirror.yandex.ru/mirrors/elastic/9/pool/main/e/elasticsearch/elasticsearch-9.1.4-amd64.deb"
LS_DEB_URL="https://mirror.yandex.ru/mirrors/elastic/9/pool/main/l/logstash/logstash-9.1.4-amd64.deb"
KB_DEB_URL="https://mirror.yandex.ru/mirrors/elastic/9/pool/main/k/kibana/kibana-9.1.4-amd64.deb"

LOGSTASH_PIPELINES_SRC="$SCRIPTS_SRC/logstash_conf/pipelines.yml"
LOGSTASH_PIPELINES_DST="/etc/logstash/pipelines.yml"

RUNNER_SCRIPT="$APP_DIR/run_daily_pipeline.sh"
SERVICE_FILE="/etc/systemd/system/digital-footprint-pipeline.service"
TIMER_FILE="/etc/systemd/system/digital-footprint-pipeline.timer"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Запусти скрипт от root"
  exit 1
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "Этот установщик рассчитан на Debian/Ubuntu"
  exit 1
fi

echo "[1/11] Установка системных пакетов"
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y \
  ca-certificates \
  curl \
  wget \
  git \
  rsync \
  gnupg \
  apt-transport-https \
  software-properties-common \
  python3 \
  python3-pip \
  python3-venv

mkdir -p "$TMP_DIR"

echo "[2/11] Скачивание и установка ELK"
cd "$TMP_DIR"

wget -O elasticsearch.deb "$ES_DEB_URL"
wget -O logstash.deb "$LS_DEB_URL"
wget -O kibana.deb "$KB_DEB_URL"

dpkg -i elasticsearch.deb || apt-get -f install -y
dpkg -i logstash.deb || apt-get -f install -y
dpkg -i kibana.deb || apt-get -f install -y

echo "[3/11] Создание структуры каталогов"
mkdir -p "$APP_DIR"
mkdir -p "$APP_DIR/result"
mkdir -p "$APP_DIR/logs"
mkdir -p "$APP_DIR/tmp"
mkdir -p /var/lib/logstash/plugins/inputs/file

for module in \
  infosearch \
  github-monitor \
  domains-monitor \
  leakcheck
do
  mkdir -p "$APP_DIR/result/$module/new"
  mkdir -p "$APP_DIR/result/$module/jsonl"
done

echo "[4/11] Клонирование или обновление репозитория"
if [[ -d "$REPO_DIR/.git" ]]; then
  git -C "$REPO_DIR" pull
else
  mkdir -p "$(dirname "$REPO_DIR")"
  git clone "$REPO_URL" "$REPO_DIR"
fi

echo "[5/11] Копирование scripts"
mkdir -p "$SCRIPTS_DST"
rsync -a --delete "$SCRIPTS_SRC/" "$SCRIPTS_DST/"

echo "[6/11] Копирование logstash-конфигов"
mkdir -p /etc/logstash/conf.d

if [[ -d "$SCRIPTS_DST/logstash_conf" ]]; then
  find "$SCRIPTS_DST/logstash_conf" -maxdepth 1 -type f -name "*.conf" -exec cp -f {} /etc/logstash/conf.d/ \;
else
  echo "Не найдена папка $SCRIPTS_DST/logstash_conf"
  exit 1
fi

if [[ -f "$LOGSTASH_PIPELINES_SRC" ]]; then
  cp -f "$LOGSTASH_PIPELINES_SRC" "$LOGSTASH_PIPELINES_DST"
else
  echo "Не найден файл $LOGSTASH_PIPELINES_SRC"
  exit 1
fi

echo "[7/11] Настройка Python virtualenv"
python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel

if [[ -f "$REPO_DIR/requirements.txt" ]]; then
  "$VENV_DIR/bin/pip" install -r "$REPO_DIR/requirements.txt"
else
  echo "В корне репозитория отсутствует requirements.txt: $REPO_DIR/requirements.txt"
  exit 1
fi

echo "[8/11] Создание runner script"
cat > "$RUNNER_SCRIPT" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/Digital_Footprint_System"
SCRIPTS_DIR="$APP_DIR/scripts"
VENV_DIR="$APP_DIR/venv"
LOG_DIR="$APP_DIR/logs"

mkdir -p "$LOG_DIR"

MAIN_LOG="$LOG_DIR/daily_pipeline.log"
COLLECT_LOG="$LOG_DIR/collect.log"
CONVERTERS_LOG="$LOG_DIR/converters.log"

exec >> "$MAIN_LOG" 2>&1

echo "===== START $(date '+%F %T') ====="

run_python_tree() {
  local target_dir="$1"
  local target_log="$2"
  local label="$3"

  if [[ ! -d "$target_dir" ]]; then
    echo "[SKIP] $label directory not found: $target_dir"
    return 0
  fi

  find "$target_dir" -type f -name "*.py" | sort | while read -r script; do
    echo "[$label] RUN $script"
    {
      echo "===== $label START $script $(date '+%F %T') ====="
      "$VENV_DIR/bin/python" "$script"
      echo "===== $label END   $script $(date '+%F %T') ====="
    } >> "$target_log" 2>&1
  done
}

run_python_tree "$SCRIPTS_DIR/collect" "$COLLECT_LOG" "COLLECT"
run_python_tree "$SCRIPTS_DIR/converters" "$CONVERTERS_LOG" "CONVERTER"

if systemctl is-enabled elasticsearch >/dev/null 2>&1; then
  systemctl restart elasticsearch || true
fi

if systemctl is-enabled logstash >/dev/null 2>&1; then
  systemctl restart logstash || true
fi

if systemctl is-enabled kibana >/dev/null 2>&1; then
  systemctl restart kibana || true
fi

echo "===== END $(date '+%F %T') ====="
EOF

chmod +x "$RUNNER_SCRIPT"

echo "[9/11] Создание systemd service"
cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Digital Footprint daily pipeline
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=root
WorkingDirectory=$APP_DIR
ExecStart=$RUNNER_SCRIPT
Nice=10
EOF

echo "[10/11] Создание systemd timer"
cat > "$TIMER_FILE" <<'EOF'
[Unit]
Description=Run Digital Footprint pipeline daily at 18:00

[Timer]
OnCalendar=*-*-* 18:00:00
Persistent=true
Unit=digital-footprint-pipeline.service

[Install]
WantedBy=timers.target
EOF

echo "[11/11] Включение сервисов"
systemctl daemon-reload

systemctl enable elasticsearch || true
systemctl enable logstash || true
systemctl enable kibana || true

systemctl restart elasticsearch || true
systemctl restart logstash || true
systemctl restart kibana || true

systemctl enable digital-footprint-pipeline.timer
systemctl restart digital-footprint-pipeline.timer

echo
echo "Установка завершена"
echo "Репозиторий:   $REPO_DIR"
echo "Скрипты:       $SCRIPTS_DST"
echo "Virtualenv:    $VENV_DIR"
echo "Runner:        $RUNNER_SCRIPT"
echo "Service:       $SERVICE_FILE"
echo "Timer:         $TIMER_FILE"
echo
echo "Проверка таймера:"
echo "  systemctl list-timers | grep digital-footprint"
echo
echo "Ручной запуск:"
echo "  systemctl start digital-footprint-pipeline.service"
