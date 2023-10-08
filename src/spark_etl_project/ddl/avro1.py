import sys
import findspark
findspark.init()
import os
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import (StructType, StructField as Fld,
                               DateType as Date, FloatType as Float)
from pyspark.sql.functions import col

spark = SparkSession \
        .builder \
        .config("spark.jars", r"C:\Users\Dell\Downloads\spark-avro_2.12-3.1.1.jar") \
        .getOrCreate()
# gs://demo_011/spark-avro_2.12-3.1.1.jar
sc = spark.sparkContext
sqlContext = SQLContext(sc)
data = [{
    'col1': 'Category A',
    'col2': 100
}, {
    'col1': 'Category B',
    'col2': 200
}, {
    'col1': 'Category C',
    'col2': 300
}]

df = spark.createDataFrame(data)
df.show()
# df.write.format("csv").mode("overwrite").save(r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\df.csv")


# Save as Avro
df.coalesce(1).write.partitionBy("col1",).format("avro").mode('overwrite').save("fcff.avro")