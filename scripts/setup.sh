#!/usr/bin/env bash
# Запускать на edge-ноде из корня репозитория.
set -euo pipefail
cd "$(dirname "$0")/.."
KEY="${SSH_KEY:-$HOME/.ssh/team_internal}"
SPARK_NODE="${SPARK_NODE:-team-27-01}"
HW4_DIR="${HW4_DIR:-/opt/spark-yarn-practical-4}"
SSH_OPTS=(-i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)

echo "[1/4] Проверяем Spark-узел и ДЗ 4"
ssh "${SSH_OPTS[@]}" "team@$SPARK_NODE" "test -x $HW4_DIR/.venv/bin/spark-submit && test -f $HW4_DIR/config/spark-defaults.conf" \
  || { echo "На $SPARK_NODE нет Spark из ДЗ 4 ($HW4_DIR)"; exit 1; }

echo "[2/4] Ставим Prefect в venv на edge"
rm -rf .venv
python3 -m venv .venv || {
  sudo apt-get update -y && sudo apt-get install -y python3-venv
  rm -rf .venv && python3 -m venv .venv
}
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt

echo "[3/4] Копируем Spark-задачу на $SPARK_NODE"
ssh "${SSH_OPTS[@]}" "team@$SPARK_NODE" 'mkdir -p ~/spark-prefect-practical-5'
scp "${SSH_OPTS[@]}" -r remote/app remote/scripts "team@$SPARK_NODE:spark-prefect-practical-5/"
ssh "${SSH_OPTS[@]}" "team@$SPARK_NODE" 'chmod +x ~/spark-prefect-practical-5/scripts/*.sh'

echo "[4/4] Проверяем Prefect"
.venv/bin/prefect version | head -3
echo "Готово, дальше scripts/run.sh"
