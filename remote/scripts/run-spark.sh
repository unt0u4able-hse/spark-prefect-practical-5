#!/usr/bin/env bash
# Запускает Spark на YARN с team-27-01. Берём venv, конфиги и туннель из ДЗ 4.
set -euo pipefail
HW4_DIR="${HW4_DIR:-/opt/spark-yarn-practical-4}"
cd "$(dirname "$0")/.."
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_CONF_DIR="$HW4_DIR/config" YARN_CONF_DIR="$HW4_DIR/config"
export SPARK_CONF_DIR="$HW4_DIR/config" HADOOP_USER_NAME=spark
export PYSPARK_PYTHON="$HW4_DIR/.venv/bin/python"
mkdir -p logs
exec "$HW4_DIR/.venv/bin/spark-submit" --master yarn --deploy-mode client \
  --properties-file "$HW4_DIR/config/spark-defaults.conf" --driver-memory 768m \
  app/etl_job.py "$@"
