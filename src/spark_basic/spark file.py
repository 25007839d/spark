import findspark
findspark.init()

'''
from pyspark.context import SparkContext
from pyspark.ddl.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

spark = SparkSession.builder \
       .master("local[1]") \
       .appName("SparkRDD") \
       .config("spark.driver.bindAddress","localhost")\
       .config("spark.ui.port","4050")\
       .getOrCreate()
inputrdd = spark.sparkContext.textFile("input/KB.txt")
input_flatMap=inputrdd.flatMap(lambda x:x .split(","))
gender_fliterrdd = input_flatMap.filter(lambda x:x in('m','f'))
gender_maprdd = gender_fliterrdd.map(lambda x:(x,1))
print(gender_maprdd.reduceByKey(lambda x,y:x+y).collect())

'''
'''
from pyspark import SparkContext
sc = SparkContext("local", "ForEach app")
words = sc.parallelize (
   ["scala",
   "java",
   "hadoop",
   "spark",
   "akka",
   "spark vs hadoop",
   "pyspark",
   "pyspark and spark"]
)
def f(x): print(x)
'''
'''
from pyspark.ddl.types import StructType, StructField, StringType, IntegerType
from pyspark.context import SparkContext
from pyspark.ddl.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
data2 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
         ]

schema = StructType([ \
   StructField("firstname", StringType(), True), \
   StructField("middlename", StringType(), True), \
   StructField("lastname", StringType(), True), \
   StructField("id", StringType(), True), \
   StructField("gender", StringType(), True), \
   StructField("salary", IntegerType(), True) \
   ])
'''


# from pyspark.context import SparkContext
# from pyspark.sql.session import SparkSession
# sc = SparkContext('local')
# spark = SparkSession(sc)
#
# from pyspark.sql.types import *
# if __name__ == '__main__':
#  dataschema1 = StructType([StructField("id", dataType=IntegerType()),
#                           StructField("fname", dataType=StringType()),
#                           StructField("lname", dataType=StringType()),
#                           StructField("age", StringType()),
#                           StructField("gender", StringType()),
#
#                           ])
#
#  csvwithschemadf = spark.read.text(
#     path=r"C:\Users\RITU\OneDrive\Desktop\New Text Document.txt", schema=dataschema1,
#     header=True)
#  csvwithschemadf.printSchema()
#  csvwithschemadf.show()
#
#  inputdf = spark.createDataFrame(csvwithschemadf,dataschema1)
#  inputdf.show()
#  inputdf.printSchema()

# from pyspark.sql.session import SparkSession

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

spark = SparkSession.builder.master("local[*]").getOrCreate()
rdd = spark.sparkContext.parallelize([1,4,6])
print(rdd.collect())
