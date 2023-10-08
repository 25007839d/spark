import findspark
findspark.init()
from pyspark.sql.functions import monotonically_increasing_id, col, lpad, concat, lit, substring, regexp_replace, count, \
    countDistinct

from pyspark.sql import *
spark = SparkSession.builder\
  .appName('BigNumeric')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.26.0.jar')\
  .getOrCreate()

df = spark.read.format("bigquery").load({project}.{dataset}.{table_name})