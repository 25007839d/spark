
import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

rdd = spark.sparkContext.parallelize(data)
df=rdd.toDF()
#df.show()
def state_convert(code):
    return broadcastStates.value[code]

result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()
#print(result)


#2. Convert PySpark RDD to DataFrame
name = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(name)
df3 = rdd.toDF()
#df3.printSchema()
#df3.show(truncate=False)


# load multipile file
from pyspark.sql import SparkSession
from pyspark.sql.types import *
if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("PYSPARK 2404") \
        .master("local[*]")\
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.ui.port", "4050") \
        .getOrCreate()
    dataschema1 = StructType([StructField("id", dataType=IntegerType()),
                              StructField("fname", dataType=StringType()),
                              StructField("lname", dataType=StringType()),
                              StructField("age", IntegerType()),
                              StructField("gender", StringType()),
                              StructField("deptno", IntegerType()),
                              StructField("salary", LongType())
                              ])



# Create RDD from external Data source by 3 types
    df = spark.read.csv(r"C:\Users\erdus\PycharmProjects\pythonProject\KB.txt",schema=dataschema1)
    df1 = spark.read.format("csv").load(r"C:\Users\erdus\PycharmProjects\pythonProject\KB.txt",schema=dataschema1)

    df2 = spark.read.option("header", True) \
        .csv(r"C:\Users\erdus\PycharmProjects\pythonProject\KB.txt",schema=dataschema1)


    #df.printSchema()
    df.show()
    #df1.show()
    #df2.show()
 # write csv file

    df.write.option("header", True).csv(r"C:\Users\RITU\PycharmProjects\pythonProject\file1")

    #df2.write.options(header='True', delimiter=',').csv(r"C:\Users\RITU\PycharmProjects\pythonProject/df1")

    # write dataframe in csv file
    df.write.csv(r"C:\Users\RITU\PycharmProjects\pythonProject\file", sep="\\n")

    # # write dataframe in JSON file
    #df.write.option("header", True).orc(r"C:\Users\RITU\PycharmProjects\pythonProject\file2")
    #df.write.json(r"C:\Users\RITU\PycharmProjects\pythonProject\json")

    # write dataframe in orc file
    #df.write.option("header", True).orc(r"C:\Users\RITU\PycharmProjects\pythonProject\orc")
    #
    # # parquet file

    #df.write.option("header", True).parquet(r"C:\Users\RITU\PycharmProjects\pythonProject\parquet")

    # excle file read
    #df=spark.read.option("header",True).csv(r"C:\Users\RITU\PycharmProjects\pythonProject\New Microsoft Excel Worksheet.xlsx",schema=dataschema1)
    #df.printSchema()
    #df.show()

    # # create a dataframe from json
    #df = spark.read.json(r"C:\Users\RITU\PycharmProjects\pythonProject\json\part-00000-83f3f1a1-9168-4578-b809-c0b6bf8641f2-c000.json")
    #df.printSchema()
    #df.show()

    # # Parquet
    # * use for select perticulor file
    #df = spark.read.parquet(r"C:\Users\RITU\PycharmProjects\pythonProject\parquet\*")
    #df.printSchema()
    #df.show()

    # binary data frame
    #df = spark.read.__doc__(r"C:\Users\RITU\OneDrive\Desktop\ddl\assissgnment.docx"*")
    #df.printSchema()
    #df.show()

    # #create empty dataframe
    #rdd1 = spark.sparkContext.parallelize([])
    # print(rdd1.collect())
    #df1 = spark.createDataFrame(rdd1, schema=dataschema1)
    # df1.show()

# # select function
#df.cache()
#df.printSchema()
#df.show()
#df.select(df.fname).show()
#df.select(df.fname.alias("firstname")).show()
#df.select(("fname"),("lname")).show()
#df.select(["fname","lname"]).show()

# # dealing with nested data
# df = spark.read.format("csv").load(r"C:\Users\erdus\PycharmProjects\pythonProject\KB.txt",schema=dataschema1)
#df.printSchema()
#df.select(df.age).show()
#df.select(["fname", "lname"]).show()
#df.select("*").show()

#print(df.columns)

    # withColumns()
#df.show()
    # # existing column value change
#Display full column contents
#df.show(2,truncate=1,vertical=True)

# withColumnRenamed
#df.withColumnRenamed("fname","firstname").show()

# # filter

#df.filter(df.gender == "M").show(truncate=False)
# not equals condition
#df.filter(df.gender != "F") .show(truncate=False)
#df.filter(~(df.gender == "F")) .show(truncate=False)

#df.filter(df.gender == "M" & df.salary > 25000).show(truncate=False)

#Filter multiple condition
#df.filter( (df.gender  == "M") & (df.age  == 29) ) .show(truncate=False)

#Filter IS IN List values
li=["M","o"]
#df.filter(df.gender.isin(li)).show()

# Using startswith
#df.filter((df.fname.startswith("Y")) | (df.fname.startswith("R")) | (df.fname.startswith("R"))).show()

# like - SQL LIKE pattern
#df2.filter(df2.fname.like("%r%")).show()

# drop(), dropDuplicate() distinct()f
from pyspark.sql import Row

data = [Row(name='Ajay', age=20),
Row(name="Satish", age=25),
Row(name='Ajay', age=20),
Row(name='Ajay', age=30)]
# df1 = spark.sparkContext.parallelize(data).toDF()
#df1.show()
#df1.printSchema()


# # distinct() check all colum
#df1.distinct().show()
#
# # droptDuplicate()
#df1.dropDuplicates(['name']).show()

# # drop
#df1.drop('age').show()
#df1.distinct().show()
# # groupby() -- Aggregate function
#csvwithschemadf.groupby('deptno').avg('salary').show()

# Join
data1 = [Row(deptno=11, deptname='HR')]

deptdf = spark.sparkContext.parallelize(data1).toDF()
#deptdf.printSchema()
  # inner, left , right, full , semi
df.join(deptdf,on=df.deptno ==df1.deptno,how='inner').select(["id", "fname", "lname", df.deptno, "deptname"]).show()
df.join(deptdf,on=df.deptno==deptdf.deptno,how='left').select(["id","fname","lname",df.deptno,"deptname"]).show()
df.join(deptdf,on=df.deptno==deptdf.deptno,how='right').select(["id","fname","lname",df.deptno,"deptname"]).show()
df.join(deptdf,on=df.deptno==deptdf.deptno,how='full').select(["id","fname","lname",df.deptno,"deptname"]).show()
#antijoin
#df.join(deptdf,on=df.deptno==deptdf.deptno,how='left_anti').show()

     # union

data2 = [Row(deptno=11, deptname='HR'),
           Row(deptno=14, deptname='IT'),
            Row(deptno=13, deptname='IT'),
              ]
deptdf1 = spark.sparkContext.parallelize(data2).toDF()
#deptdf1.printSchema()
#deptdf.union(deptdf1).show()
#deptdf.unionAll(deptdf1).show()
#deptdf.unionAll(deptdf1).distinct().show()

#deptdf.show()
# df.show()
#df1.show()
#df3.show()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import*
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df.createOrReplaceTempView("PERSON_DATA")

groupDF = spark.sql("SELECT gender, count(*) from PERSON_DATA group by gender")
groupDF.show()

df.printSchema()
groupDF.show()

groupDF = spark.sql("SELECT max(salary) from PERSON_DATA")
groupDF.show()
df = spark.sql("SELECT max(salary) from PERSON_DATA^^^(SELECT max(salary) from PERSON_DATA)")
df.show()

