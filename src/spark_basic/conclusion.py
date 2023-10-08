from pyspark.sql import SparkSession
from pyspark.sql.types import*
from pyspark import *
sc=SparkContext.getOrCreate()
spark = SparkSession(sc)

rdd=sc.textFile(r'C:\Users\erdus\PycharmProjects\pythonProject\ac.txt')
print(rdd.collect())
rdd1 = rdd.flatMap(lambda x: x.split(' '))
print(rdd1.collect())
# transformation
y="we,so,not,is,a,vat,nat,chat,nos,sute,very,poor"
column= ['string','int']
column1= ['animal','int']
fil=rdd1.filter(lambda x: x not in y )
fil2=rdd1.filter(lambda x: x  in y )
rdd3 = fil.map(lambda x: (x,1))
rdd4= fil2.map(lambda x: (x,1))
#
# df1=spark.createDataFrame(data=rdd3,schema=column)
# df2=spark.createDataFrame(data=rdd4,schema=column1)

#1. Create PySpark MapType

from pyspark.sql.types import StringType, MapType
mapCol = MapType(StringType(),StringType(),False)

schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])
dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]
df = spark.createDataFrame(data=dataDictionary, schema = schema)
# df.printSchema()
# df.show(truncate=False)


df3=df.rdd.map(lambda x: \
    (x.name,x.properties["hair"],x.properties["eye"])) \
    .toDF(["name","hair","eye"])
df3.printSchema()
df3.show()


# df.withColumn("hair",df.properties.getItem("hair")) \
#   .withColumn("eye",df.properties.getItem("eye")) \
#   .drop("properties") \
#   .show()

# df.withColumn("hair",df.properties["hair"]) \
#   .withColumn("eye",df.properties["eye"]) \
#   .drop("properties") \
#   .show()
from pyspark.sql.functions import explode
# df.select(df.name,explode(df.properties)).show()

from pyspark.sql.functions import explode,map_keys
keysDF = df.select(explode(map_keys(df.properties))).distinct()
# keysList = keysDF.rdd.map(lambda x:x[0]).collect()
# print(keysList)