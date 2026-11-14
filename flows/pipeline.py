"""ДЗ 5: поток Prefect для обработки данных в Spark на YARN.

Запускается на edge-ноде, к остальным узлам ходит по ssh (ключ team_internal):
team-27-nn проверяем YARN и HDFS, team-27-01 запускаем spark-submit,
team-27-00 регистрируем и читаем таблицу через Beeline.
"""
import json
import os
import shlex
import subprocess
import time
from decimal import Decimal
from pathlib import Path

from prefect import flow, get_run_logger, task

KEY = os.environ.get('SSH_KEY', os.path.expanduser('~/.ssh/team_internal'))
NN = os.environ.get('NN_NODE', 'team-27-nn')
SPARK_NODE = os.environ.get('SPARK_NODE', 'team-27-01')
HIVE_NODE = os.environ.get('HIVE_NODE', 'team-27-00')
REMOTE_DIR = os.environ.get('REMOTE_DIR', '$HOME/spark-prefect-practical-5')
HIVE_DIR = os.environ.get('HIVE_DIR', '/opt/hive-cluster-practical-3')
NN_CONTAINER = os.environ.get('NN_CONTAINER', 'hdfs-lab-namenode-1')
LOGS = Path(__file__).resolve().parents[1] / 'logs'


def ssh(host: str, command: str, timeout: int = 1800) -> str:
    cmd = ['ssh', '-i', KEY, '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=accept-new',
           '-o', 'ServerAliveInterval=15', f'team@{host}', command]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if res.returncode != 0:
        raise RuntimeError(f'ssh {host} завершился с кодом {res.returncode}\n'
                           f'stderr: {res.stderr.strip()[-2000:]}\nstdout: {res.stdout.strip()[-1000:]}')
    return res.stdout


def beeline(sql: str) -> str:
    return ssh(HIVE_NODE, f'cd {HIVE_DIR} && sudo scripts/beeline.sh -e {shlex.quote(sql)}')


def nonempty(text: str) -> list:
    return [line for line in text.splitlines() if line.strip()]


@task(name='check-yarn-hdfs-input', retries=2, retry_delay_seconds=15)
def check_cluster(input_path: str) -> dict:
    """Перед запуском смотрим, что YARN и HDFS живы, туннель поднят и данные на месте."""
    log = get_run_logger()
    yarn = json.loads(ssh(NN, 'curl -fsS http://localhost:8088/ws/v1/cluster/info'))['clusterInfo']
    assert yarn['state'] == 'STARTED', yarn
    nodes_json = json.loads(ssh(NN, 'curl -fsS http://localhost:8088/ws/v1/cluster/nodes'))
    running = [n for n in (nodes_json.get('nodes') or {}).get('node', []) if n['state'] == 'RUNNING']
    assert running, 'нет живых NodeManager'

    jmx = shlex.quote('http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState')
    hdfs = json.loads(ssh(NN, f'curl -fsS {jmx}'))['beans'][0]
    assert hdfs['NumLiveDataNodes'] >= 1, hdfs

    tunnel = nonempty(ssh(SPARK_NODE, 'systemctl is-active wg-quick@wg-spark spark-yarn-route'))
    assert tunnel and all(s == 'active' for s in tunnel), tunnel

    listing = ssh(NN, f'sudo docker exec {NN_CONTAINER} hdfs dfs -ls -R {shlex.quote(input_path)}')
    files = [l for l in listing.splitlines() if l.startswith('-')]
    assert files, f'в {input_path} нет файлов'

    info = {'yarn_nodes': len(running), 'hdfs_live_datanodes': hdfs['NumLiveDataNodes'],
            'input_files': len(files)}
    log.info('Кластер в порядке: %s', info)
    return info


@task(name='spark-etl-on-yarn', timeout_seconds=1800)
def run_spark_job(input_path: str, output_path: str, table: str) -> dict:
    """Шаги 1-5 задания: запускаем etl_job.py на YARN и забираем отчёт из лога."""
    log = get_run_logger()
    args = (f'--input hdfs://namenode:8020{input_path} '
            f'--output hdfs://namenode:8020{output_path} --table {shlex.quote(table)}')
    cmd = (f'cd {REMOTE_DIR} && mkdir -p logs && '
           f'{{ ./scripts/run-spark.sh {args} > logs/spark-submit.log 2>&1 '
           f'|| {{ tail -n 40 logs/spark-submit.log >&2; exit 1; }}; }} && '
           f"grep -m1 '^SPARK_RESULT ' logs/spark-submit.log")
    out = ssh(SPARK_NODE, cmd)
    result = json.loads(out.split('SPARK_RESULT ', 1)[1])
    log.info('Spark завершён: %s, строк на входе %s, в таблице %s',
             result['application_id'], result['input_rows'], len(result['result_rows']))
    return result


@task(name='register-hive-table')
def register_table(output_path: str, db: str, table: str) -> None:
    """Внешняя таблица в Hive поверх parquet-файлов, которые записал Spark."""
    sql = (f'CREATE DATABASE IF NOT EXISTS {db}; '
           f'CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} ('
           'order_count BIGINT, gross_amount DECIMAL(18,2), avg_amount DECIMAL(18,2), '
           'product_count BIGINT, vat_amount DECIMAL(18,2)) '
           'PARTITIONED BY (dt DATE, country STRING) STORED AS PARQUET '
           f"LOCATION 'hdfs://namenode:8020{output_path}'; "
           f'MSCK REPAIR TABLE {db}.{table} SYNC PARTITIONS;')
    beeline(sql)


@task(name='verify-result')
def verify(result: dict, db: str, table: str) -> dict:
    """Сверяем Hive со Spark (строки, партиции, сумма) и смотрим статус приложения в YARN."""
    log = get_run_logger()
    rows = nonempty(beeline(f'SELECT dt,country,order_count,gross_amount FROM {db}.{table};'))
    parts = nonempty(beeline(f'SHOW PARTITIONS {db}.{table};'))
    assert len(rows) == len(result['result_rows']), (len(rows), len(result['result_rows']))
    assert len(parts) == len(result['partitions']), (parts, result['partitions'])

    hive_total = sum(Decimal(l.split('\t')[3]) for l in rows)
    spark_total = sum(Decimal(r[3]) for r in result['result_rows'])
    assert hive_total == spark_total, (hive_total, spark_total)

    aid = result['application_id']
    app = {}
    for _ in range(10):
        app = json.loads(ssh(NN, f'curl -fsS http://localhost:8088/ws/v1/cluster/apps/{aid}'))['app']
        if app['state'] == 'FINISHED':
            break
        time.sleep(3)
    assert app['applicationType'] == 'SPARK' and app['finalStatus'] == 'SUCCEEDED', app

    summary = {'status': 'PASS', 'application_id': aid, 'yarn_state': app['state'],
               'yarn_final_status': app['finalStatus'], 'input_rows': result['input_rows'],
               'hive_rows': len(rows), 'partitions': parts, 'gross_total': str(hive_total)}
    log.info('Всё сошлось: %s', summary)
    return summary


@flow(name='spark-yarn-hdfs-pipeline', log_prints=True)
def pipeline(input_path: str = '/user/hive/practical3/orders',
             output_path: str = '/user/spark/practical4/prefect_daily_sales',
             spark_table: str = 'daily_sales_p5',
             hive_db: str = 'practical5',
             hive_table: str = 'daily_sales') -> dict:
    check_cluster(input_path)
    result = run_spark_job(input_path, output_path, spark_table)
    register_table(output_path, hive_db, hive_table)
    summary = verify(result, hive_db, hive_table)
    LOGS.mkdir(exist_ok=True)
    (LOGS / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    (LOGS / 'spark-result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


if __name__ == '__main__':
    pipeline()
