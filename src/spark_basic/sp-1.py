#There are three ways to create an RDD in Spark.

#1.Parallelizing already existing collection in driver program.


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.readwriter import DataFrameReader
sc = SparkContext("local","app")
spark = SparkSession(sc)



# words = sc.parallelize (
#    ["scala",
#
#    "java",
#    "hadoop",
#    "spark",
#    "akka",
#    "spark vs hadoop",
#    "pyspark",
#    "pyspark and spark"]
# )
# counts = words.count()
# print ("Number of elements in RDD -> %i" % (counts))


# sum of salary per department by Data frame


simpleData = [(1,"Yesh","Patil",29,"M",11,20000),
              (2,"Ram","Wagh",30,"M",12,3000000),
              (3,"Sita","Patil",29,"F",11,50000),
              (4,"Kiran","XYZ",33,"F",11,40052),
              (5,"Savita","Waghmare",35,"F",12,800000)
              ]

schema = ["eid","first_name","last_nmae","age","sex","d_id","salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
# df.printSchema()
# df.show(truncate=False)
# df.groupBy("d_id").sum("salary").show(truncate=False)
# df.groupBy("d_id").avg("salary").show(truncate=False)
# df.groupBy("d_id").max("salary").show(truncate=False)
# df.groupBy("d_id","eid").max("salary").show(truncate=False)
# df.select('d_id','salary','first_name','eid').groupby("d_id","eid").max('salary').show()
"""ddl"""

"""window fun groupBy"""

from pyspark.sql.window import Window # for window function
from pyspark.sql.functions import * # for column

windowSpec  = Window.partitionBy("d_id",'eid').orderBy("salary")
df.withColumn("row_number",row_number().over(windowSpec)).where(col("row_number")==1).show()

"""with column"""
df.withColumn('max',row_number().over(windowSpec)).groupby("d_id","eid").agg(max('salary')).show()

"""SQL query"""
# df.createOrReplaceTempView("table")
# spark.ddl("select d_id, salary from table group by salary, d_id order by d_id ").show()



#creat dataframe by file
#2.Referencing a dataset in an external storage system (e.g. HDFS, Hbase, shared file system).
from pyspark.sql import SparkSession
from pyspark.sql.types import *

dataschema1 = StructType([StructField("id", dataType=IntegerType()),
                              StructField("fname", dataType=StringType()),
                              StructField("lname", dataType=StringType()),
                              StructField("age", IntegerType()),
                              StructField("gender", StringType()),
                              StructField("deptno", IntegerType()),
                              StructField("salary", LongType())
                              ])

csvwithschemadf = spark.read.csv( path=r"C:\Users\erdus\PycharmProjects\pythonProject\KB.txt", schema=dataschema1, header=True)
    # csvwithschemadf.printSchema()
    # csvwithschemadf.show()

# no need of  spark = SparkSession.builder



from pyspark.sql.types import *
dataschema1 = StructType([StructField("id", dataType=IntegerType()),
                          StructField("fname", dataType=StringType()),
                          StructField("lname", dataType=StringType()),
                          StructField("age", StringType()),
                          StructField("gender", StringType()),

                          ])
df = spark.read.csv(
path=r"C:\Users\erdus\PycharmProjects\pythonProject\KB.txt", schema=dataschema1, header=True)
# csvwithschemadf.printSchema()
# csvwithschemadf.show()
