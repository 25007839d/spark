from pyspark.sql import SparkSession
from pyspark.sql.types import*
from pyspark import *
sc=SparkContext.getOrCreate()
spark = SparkSession(sc)

# window function
# 1.ranking functions
# 2.analytic functions
# 3.aggregate functions

simpleData = (("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("James", "Sales", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100) \
              )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
# df.printSchema()
# df.show(truncate=False)

# 2. PySpark Window Ranking functions
# 2.1 row_number Window Function

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec  = Window.partitionBy("department",'employee_name').orderBy("salary")
df.withColumn("row_number",row_number().over(windowSpec)).show()

#2.3 dense_rank Window Function

from  pyspark.sql.functions import dense_rank
# df.withColumn("xxx.com",dense_rank().over(windowSpec)) \
#     .show()
#
#
# """rank"""
from pyspark.sql.functions import rank
# df.withColumn("rank",rank().over(windowSpec)).show()


""" percent_rank """
from pyspark.sql.functions import percent_rank
#df.withColumn("percent_rank",percent_rank().over(windowSpec)) \
    #.show()

"""ntile"""
from pyspark.sql.functions import ntile
#df.withColumn("ntile",ntile(4).over(windowSpec)) \
    # .show()
#. PySpark Window Analytic functions
#3.1 cume_dist Window Function

""" cume_dist """
from pyspark.sql.functions import cume_dist
#df.withColumn("cume_dist",cume_dist().over(windowSpec)).show()



"""lag"""
from pyspark.sql.functions import lag
df.withColumn("lag",lag("salary",1).over(windowSpec)) \
 # .show()
#
#
windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number
df.withColumn("row",row_number().over(windowSpec)) \
   .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
   .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
   .where(col("row")==1).select("department","avg","sum","min","max") \
#   .show()
"""lead"""
from pyspark.sql.functions import lead
df.withColumn("lead",lead("salary",2).over(windowSpec)) \
    # .show()

# Source Code of Window Functions Example



