#!/usr/bin/env bash
# Install Apache Airflow (Docker Compose, LocalExecutor) di server Linux UTC.
# Bisnis/tarif tetap Asia/Jakarta. Jalankan dari root proyek:
#   bash scripts/install_airflow_linux.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f docker-compose.airflow.yaml)
ENV_FILE="$ROOT/.env"

red() { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
info() { printf '\033[36m%s\033[0m\n' "$*"; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    red "Perintah '$1' tidak ditemukan. Install dulu, lalu ulangi."
    exit 1
  }
}

ensure_env_line() {
  local key="$1"
  local value="$2"
  if grep -qE "^${key}=" "$ENV_FILE" 2>/dev/null; then
    return 0
  fi
  printf '%s=%s\n' "$key" "$value" >>"$ENV_FILE"
}

set_env_if_empty() {
  local key="$1"
  local value="$2"
  if grep -qE "^${key}=$" "$ENV_FILE" 2>/dev/null || ! grep -qE "^${key}=" "$ENV_FILE" 2>/dev/null; then
    if grep -qE "^${key}=" "$ENV_FILE" 2>/dev/null; then
      sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
    else
      printf '%s=%s\n' "$key" "$value" >>"$ENV_FILE"
    fi
  fi
}

info "=== Cek prasyarat ==="
need_cmd docker
need_cmd openssl
docker compose version >/dev/null
if [[ "$(uname -s)" != "Linux" ]]; then
  red "Script ini untuk server Linux. Host sekarang: $(uname -s)"
  exit 1
fi
if [[ "$(id -u)" -eq 0 ]]; then
  red "Jangan jalankan sebagai root. Pakai user di grup docker."
  exit 1
fi

HOST_TZ="$(timedatectl show -p Timezone --value 2>/dev/null || cat /etc/timezone 2>/dev/null || echo unknown)"
info "Timezone OS: ${HOST_TZ}"
if [[ "$HOST_TZ" != "UTC" && "$HOST_TZ" != "Etc/UTC" ]]; then
  red "OS bukan UTC (sekarang ${HOST_TZ})."
  red "Biarkan OS UTC. Jangan ganti ke Asia/Jakarta — DAG sudah tz-aware WIB."
fi

if [[ ! -f "$ENV_FILE" ]]; then
  if [[ -f "$ROOT/.env.example" ]]; then
    cp "$ROOT/.env.example" "$ENV_FILE"
    red "File .env baru disalin dari .env.example. Isi SOURCE_DB_URL / DATAMART_DB_URL dulu."
    exit 1
  fi
  red "Tidak ada .env. Buat dari .env.example."
  exit 1
fi

mkdir -p "$ROOT/dags" "$ROOT/logs" "$ROOT/plugins"

info "=== Tulis variabel Airflow ke .env (tanpa menimpa yang sudah ada) ==="
ensure_env_line "AIRFLOW_UID" "$(id -u)"
sed -i "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$(id -u)|" "$ENV_FILE"

if ! grep -qE "^AIRFLOW_FERNET_KEY=.+" "$ENV_FILE"; then
  info "Generate AIRFLOW_FERNET_KEY..."
  FERNET="$(docker run --rm apache/airflow:2.11.0-python3.11 python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
  set_env_if_empty "AIRFLOW_FERNET_KEY" "$FERNET"
fi

if ! grep -qE "^_AIRFLOW_WWW_USER_PASSWORD=.+" "$ENV_FILE"; then
  WWW_PASS="$(openssl rand -base64 18 | tr -d '/+=' | cut -c1-20)"
  set_env_if_empty "_AIRFLOW_WWW_USER_PASSWORD" "$WWW_PASS"
  info "Password UI Airflow (simpan): ${WWW_PASS}"
fi

ensure_env_line "_AIRFLOW_WWW_USER_USERNAME" "admin"
if ! grep -qE "^AIRFLOW_POSTGRES_PASSWORD=.+" "$ENV_FILE"; then
  set_env_if_empty "AIRFLOW_POSTGRES_PASSWORD" "$(openssl rand -base64 18 | tr -d '/+=' | cut -c1-20)"
fi
ensure_env_line "AIRFLOW_WEBSERVER_PORT" "8080"
ensure_env_line "AIRFLOW__CORE__DEFAULT_TIMEZONE" "utc"
ensure_env_line "AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE" "Asia/Jakarta"

if ! grep -qE "^TIMEZONE=Asia/Jakarta" "$ENV_FILE"; then
  red "TIMEZONE di .env harus Asia/Jakarta (jam bisnis / WBP), terpisah dari TZ=UTC di container."
  ensure_env_line "TIMEZONE" "Asia/Jakarta"
fi

info "=== Build image & start stack ==="
"${COMPOSE[@]}" build
"${COMPOSE[@]}" up -d

info "Menunggu webserver sehat..."
ok=0
for _ in $(seq 1 60); do
  if "${COMPOSE[@]}" exec -T airflow-webserver curl -sf http://localhost:8080/health >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 5
done
if [[ "$ok" -ne 1 ]]; then
  red "Webserver belum sehat. Cek log:"
  echo "  ${COMPOSE[*]} logs --tail=80 airflow-init airflow-webserver airflow-scheduler"
  "${COMPOSE[@]}" ps
  exit 1
fi

"${COMPOSE[@]}" ps

WWW_USER="$(grep -E '^_AIRFLOW_WWW_USER_USERNAME=' "$ENV_FILE" | cut -d= -f2-)"
PORT="$(grep -E '^AIRFLOW_WEBSERVER_PORT=' "$ENV_FILE" | cut -d= -f2- || echo 8080)"

green "Airflow siap."
echo
echo "  UI        : http://$(hostname -I 2>/dev/null | awk '{print $1}'):${PORT:-8080}"
echo "  User      : ${WWW_USER:-admin}"
echo "  Timezone  : scheduler UTC, UI default Asia/Jakarta, ETL TIMEZONE=Asia/Jakarta"
echo
echo "Langkah berikutnya:"
echo "  1. Unpause DAG di UI: timezone_audit, etl_meter_readings, meter_runtime_daily"
echo "  2. Trigger timezone_audit sekali, pastikan log 'OK'"
echo "  3. (opsional) systemd:"
echo "       sudo cp scripts/energy-airflow.service /etc/systemd/system/"
echo "       sudo sed -i 's|/opt/energy-etl|${ROOT}|' /etc/systemd/system/energy-airflow.service"
echo "       sudo systemctl daemon-reload && sudo systemctl enable --now energy-airflow"
echo
echo "Cek status:  ${COMPOSE[*]} ps"
echo "Log:         ${COMPOSE[*]} logs -f airflow-scheduler"
echo "Stop:        ${COMPOSE[*]} down"
echo "Stop+data:   ${COMPOSE[*]} down -v"
