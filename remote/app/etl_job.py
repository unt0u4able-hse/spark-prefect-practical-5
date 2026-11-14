"""ДЗ 5: Spark на YARN читает заказы из HDFS, считает дневные продажи и сохраняет таблицу."""
import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

parser = argparse.ArgumentParser()
parser.add_argument('--input', default='hdfs://namenode:8020/user/hive/practical3/orders')
parser.add_argument('--output', default='hdfs://namenode:8020/user/spark/practical4/prefect_daily_sales')
parser.add_argument('--table', default='daily_sales_p5')
parser.add_argument('--report', default='logs/etl-result.json')
args = parser.parse_args()

# Шаг 1. Сессия Spark. master и deploy-mode задаёт spark-submit (yarn, client).
spark = SparkSession.builder.appName('team-27-hw5-prefect-spark').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
try:
    sc = spark.sparkContext
    assert sc.master == 'yarn', f'ожидался yarn, получен {sc.master}'
    assert sc.applicationId.startswith('application_'), sc.applicationId

    # Шаги 2-3. HDFS берётся из fs.defaultFS в конфигах, читаем данные из ДЗ 3.
    # dt и country Spark достаёт сам из путей dt=.../country=...
    schema = T.StructType([
        T.StructField('order_id', T.IntegerType()),
        T.StructField('product', T.StringType()),
        T.StructField('amount', T.DecimalType(10, 2)),
    ])
    source = spark.read.schema(schema).option('basePath', args.input).csv(args.input)

    # Шаг 4. Трансформации: типы, чистка строк, фильтр, агрегация, НДС.
    typed = (source
             .withColumn('dt', F.to_date('dt'))
             .withColumn('country', F.upper(F.trim('country')))
             .withColumn('product', F.lower(F.trim('product')))
             .withColumn('amount', F.col('amount').cast('decimal(10,2)')))
    valid = typed.filter(
        F.col('order_id').isNotNull() & F.col('product').isNotNull()
        & F.col('dt').isNotNull() & F.col('country').isNotNull()
        & (F.col('amount') > 0))
    input_rows = typed.count()
    valid_rows = valid.count()
    assert valid_rows > 0, 'после фильтрации не осталось строк'

    daily = (valid.groupBy('dt', 'country')
             .agg(F.count('*').alias('order_count'),
                  F.sum('amount').cast('decimal(18,2)').alias('gross_amount'),
                  F.avg('amount').cast('decimal(18,2)').alias('avg_amount'),
                  F.countDistinct('product').alias('product_count'))
             .withColumn('vat_amount',
                         (F.col('gross_amount') * F.lit(0.20)).cast('decimal(18,2)')))

    # Шаг 5. Сохраняем как таблицу: parquet в HDFS, партиции dt и country.
    (daily.write.mode('overwrite').format('parquet')
     .option('path', args.output).partitionBy('dt', 'country')
     .saveAsTable(args.table))

    rows = spark.table(args.table).orderBy('dt', 'country').collect()
    partitions = [str(r[0]) for r in spark.sql(f'SHOW PARTITIONS {args.table}').collect()]
    assert len(rows) == len(partitions) > 0, (len(rows), partitions)

    report = {
        'application_id': sc.applicationId,
        'spark_version': spark.version,
        'master': sc.master,
        'deploy_mode': sc.getConf().get('spark.submit.deployMode'),
        'input': args.input,
        'output': args.output,
        'table': args.table,
        'input_rows': input_rows,
        'valid_rows': valid_rows,
        'result_rows': [[str(r.dt), r.country, r.order_count, str(r.gross_amount),
                         str(r.avg_amount), r.product_count, str(r.vat_amount)] for r in rows],
        'partitions': partitions,
        'table_schema': spark.table(args.table).schema.simpleString(),
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print('SPARK_RESULT ' + json.dumps(report, ensure_ascii=False), flush=True)
finally:
    spark.stop()
