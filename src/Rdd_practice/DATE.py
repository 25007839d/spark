import regexp as regexp
import findspark
findspark.init()
from pyspark.sql.types import StructField


from pyspark.sql import SparkSession
from pyspark.sql.functions import*
import re

if __name__=='__main__':
    spark = SparkSession.builder.master('local[*]').getOrCreate()


    # df = spark.read.csv(r"C:\Users\Dell\Desktop\table\date.csv")
    # df.show()

    from pyspark.sql.types import *

    my_schema = StructType([
        StructField('id', LongType()),
        StructField('country', StructType([
            StructField('name', StringType()),
            StructField('capital', StringType())
        ])),
        StructField('currency', StringType())
    ])
    l = [
        (1, ('Italy','Rome'), 'euro'),
        (2, ['France',  'Paris'], 'euro'),
        (3, ['Japan','Tokyo'], 'yen')
    ]
    df = spark.createDataFrame(l, schema=my_schema)
    df.show(
    df.withColumn('country-1',col('country.name')).show()
    df.select('country.capital').show() # seprate nested fielde

    df.selectExpr("struct(id,currency) as my_struct ").show() # create struct with df
    ns=df.withColumn("new_struct",struct("id",'country'))

    ns.select('new_struct.id').show()