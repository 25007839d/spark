import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').getOrCreate()
