from pyspark.sql import SparkSession
from pyspark.sql.types import*
from pyspark import *
sc=SparkContext.getOrCreate()
spark = SparkSession(sc)
schema="id int,fname string,lname string,age int,gender string,deptno int,salary int,j_date string"
df = spark.read.csv(path=r"C:\Users\erdus\PycharmProjects\pythonProject\project\data",schema=schema)
#df.show()

from pyspark.sql import window
from pyspark.sql.functions import *
"""current_date()"""
#df.select(current_date().alias("current_date")).show(1)  # for current date

"""date_format(""" # date format to anathor date format
#df.select(col("j_date"),date_format(col("j_date"), "dd-MM-yyyy").alias("date_format")).show()


"""to_date()""" # string to date formate
# df.select(col("j_date"),to_date(col("j_date"), "yyy-MM-dd").alias("to_date")).show()


"""datediff()"""
# df.select(col("j_date"),datediff(current_date(),col("j_date")).alias("datediff")).show()


"""months_between()"""
# df.select(col("j_date"),months_between(current_date(),col("j_date")).alias("months_between")).show()



"""trunc()""" # as similiar to round function
# df.select(col("j_date"),\
#     trunc(col("j_date"),"Month").alias("Month_Trunc"),\
#     trunc(col("j_date"),"Year").alias("Month_Year"),\
#     trunc(col("j_date"),"Month").alias("Month_Trunc")\
#    ).show()


"""add_months() , date_add(), date_sub()"""
# df.select(col("j_date"),
#     add_months(col("j_date"),3).alias("add_months"),
#     add_months(col("j_date"),-3).alias("sub_months"),
#     date_add(col("j_date"),4).alias("date_add"),
#     date_sub(col("j_date"),-4).alias("date_sub")
#   ).show()


"""year(), month(), month(),next_day(), weekofyear()"""
# df.select(col("j_date"),
#      year(col("j_date")).alias("year"),
#      month(col("j_date")).alias("month"),
#      next_day(col("j_date"),"Sunday").alias("next_day"),
#      weekofyear(col("j_date")).alias("weekofyear")
#   ).show()


"""dayofweek(), dayofmonth(), dayofyear()"""
# df.select(col("j_date"),
#      dayofweek(col("j_date")).alias("dayofweek"),
#      dayofmonth(col("j_date")).alias("dayofmonth"),
#      dayofyear(col("j_date")).alias("dayofyear"),
#   ).show()



"""current_timestamp()"""


#current_timestamp()
# df.select(current_timestamp().alias("current_timestamp")
#   ).show(1,truncate=False)


"""to_timestamp()"""#Converts string timestamp to Timestamp type format.
#
# df.select(col("j_date"),
#     to_timestamp(col("j_date"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp")
#   ).show(truncate=False)


"""hour, minute,second"""
# data=[["1","2020-02-01 11:01:19.06"],["2","2019-03-01 12:01:19.406"],["3","2021-03-01 12:01:19.406"]]
# df3=spark.createDataFrame(data,["id","input"])

# df3.select(col("input"),
#     hour(col("input")).alias("hour"),
#     minute(col("input")).alias("minute"),
#     second(col("input")).alias("second")
#   ).show(truncate=False)

""" Assingment Que."""

#1. retrive record which is having maximue date with respective of id, name column
schema1 = "id int,name string,price int,date string"
df1 = spark.read.csv(path=r"C:\Users\erdus\PycharmProjects\pythonProject\project\date_assignment", schema=schema1)
#df1.show()
df1.select('*').groupBy("id","name").agg(max("date")).alias("max_s").show(truncate=False)




#2. 2. expected output as below

