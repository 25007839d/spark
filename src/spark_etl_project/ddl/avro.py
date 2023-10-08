#avro-example.py
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import avro


appName = "PySpark Example - Read and Write Avro"
master = "local"
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages   org.apache.spark:spark-avro_2.12:2.4.5  pyspark-shell'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages   org.apache.spark:spark-avro_2.11-2.4.2  pyspark-shell'



# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()


# conf = spark.sparkContext._jsc.hadoopConfiguration()
# conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
# conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
# conf.set("spark.speculation","false")

# List
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
# df.write.mode('overwrite').format("Avro").option("compression","snappy").save("fcf.avro_1")
# dataset.coalesce(1).write.format("avro").mode('overwrite').save(dataset_path)
