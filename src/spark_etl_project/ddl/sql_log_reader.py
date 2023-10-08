import findspark
findspark.init()
from pyspark.sql.functions import monotonically_increasing_id, col, lpad, concat, lit, substring, regexp_replace, count, \
    countDistinct, split, explode, when, explode_outer

from pyspark.sql import *

if __name__== "__main__":
    spark = SparkSession.builder.appName('oracle_connection')\
        .config('spark.jars',r"../ddl-connect/ojdbc8.jar")\
        .getOrCreate()

    df = spark.sparkContext.textFile("D:\Brainwork\sql\loader\consql.log")

    rdd = df.map(lambda x: x.split(',')).collect()[1]
    print(rdd)

    print(df.collect())
