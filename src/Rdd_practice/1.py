import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
import re

if __name__=='__main__':
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    rdd = spark.sparkContext.parallelize([2,3,4])


    rdd2=rdd.flatMap(lambda x: [(x, 2), (x, x)]).collect()
    print(rdd2)

    x = spark.sparkContext.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])
    def f(x): return x
    print(x.flatMapValues(f).collect())



    # rdd3 = rdd2.map(lambda x: call(x))
    #
    # print(rdd3.collect())
    # rdd1 = rdd.map(lambda x:(x,1))
    # print(rdd1.collect())
    # #
    # # rdd5 = rdd2.map(lambda x: (x[0],x[1]))
    # # print(rdd5.collect())
    #
    # rdd2 = rdd1.reduceByKey(lambda a, b: a + b).sortByKey()
    # print(rdd2.collect())
    #
    # rdd3 = rdd2.filter(map(lambda x: len(x(x[0]))>12))
    #
    # print(rdd3.collect())
    #
    #
    #
    # a=['HIVE','spark']
    #
    # rdd3 = rdd2.map(lambda x: (x[0],x[1]))
    #
    # rdd4= rdd3.filter(lambda x: a in x)
    # print(rdd3.collect())
    #
    # rdd6= rdd5.repartition(1)
    #
    # rdd6.saveAsTextFile(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\int")
    #
    # rdd5 = rdd4.map(lambda x: (x[0]))
    # # Print rdd6 result to console
    # print(rdd5.collect())
    #
    # rdd6 = rdd5.filter(lambda x: 'is' not in x)
    # print(rdd6.collect())