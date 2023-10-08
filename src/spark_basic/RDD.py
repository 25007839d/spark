
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[1]').appName('sparkcode').getOrCreate()

#sc.parallelize()
rdd_small = spark.sparkContext.parallelize([3, 1, 12, 6, 8, 10, 14, 19])
#print(rdd_small)
#print(rdd_small.collect())


# large text data set
rdd_set = spark.sparkContext.parallelize([[2, 12, 5, 19, 21],
                          [10, 19, 5, 21, 8],
                          [34, 21, 14, 8, 10],
                          [110, 89, 90, 134, 24],
                          [23, 119, 234, 34, 56]])
#print(rdd_set.first())
print(rdd_set)
#print(rdd_set.take(3))
#text file
lines = sc.textFile(r"C:\Users\KAJAL\PycharmProjects\spark\spark\stu.text")


print(lines.take(1))
print(lines.collect())
print(lines.take(2))

#.flatMap()

words = lines.flatMap(lambda x: x.split(' '))
print(words.collect())
list=[]
'''
for i in words:
    if i=="sunita":
        i="Ritu"
        list.append(i)
    else:
        list.append(i)
print(list)
'''
# wordsAsTuples = words.map(lambda x: (x.lower(), 1))
# wordsAsTuples.take(4)
# #print(wordsAsTuples.take(4))
#
# #.reduceByKey
#
# counts = wordsAsTuples.reduceByKey(lambda x, y: x+y)
#
# #print(counts.take(10))
#
# #.top()
# d=counts.top(5, lambda x: x[1])
# #print(d)
#
# #.filter()
# stop = ['a', 'aa']
# words_short = counts.filter(lambda x: x[0] not in stop)
# d1=words_short.top(5, lambda x: x[1])
# # print(d1)
#
# # .sortByKey() for alpha...
# d2=counts.sortByKey().take(10)
# # for decending order
# d3=counts.sortByKey(False).take(10)
# #print(d2)
# # for short Cap and small
# d4=counts.sortByKey().collect()
# #print(d4)
#
# #.groupByKey()
# d5=counts.groupByKey().map(lambda x: sum(x[1]))
# #print(d5.take(10))
#
# #.reduce()
# d6 = counts.reduce(lambda x, y: x+y)
# #print(d6)
# #print(counts.collect())
#
# #.mapValues()
# rdd_1 = sc.parallelize([("a", 3), ("n", 10), ("s", 5), ("l", 12)])
# # e=rdd_1.mapValues(lambda x: x/2).collect()
# #or
# # e1=rdd_1.map(lambda x: x[1]+1).collect()
# dff=rdd_1.fil
# #print(e)
# #print(e1)
#
# # mapValues for sum
# rdd_map = sc.parallelize([("a", [1, 2, 3, 4]), ("b", [10, 2, 8, 1])])
# # e1=rdd_map.mapValues(lambda x: sum(x)).collect()
# #print(e1)
#
# #.countByValue()
# e2=sc.parallelize([1, 2, 1, 3, 2, 4, 1, 4, 4]).countByValue()
# #print(e2)
#
# #.getNumPartitions() how many partition in rdd of data
# data = sc.parallelize([("p",5),("q",0),("r", 10),("q",3)])
# #print(data.getNumPartitions())
# #print(data.glom().collect())
#
# #.zip()
# rd11 = sc.parallelize(["a", "b", "c", "d", "e"])
# rdda = sc.parallelize([1, 2, 3, 4, 5])
# rda_11 = rdda.zip(rd11)
# #print(rda_11.collect())
#
#
# from pyspark.ddl import SparkSession
# spark = SparkSession.builder \
#         .appName("PYSPARK 2404") \
#         .master("local[*]")\
#         .config("spark.driver.bindAddress", "localhost") \
#         .config("spark.ui.port", "4050") \
#         .getOrCreate()
# r1 = [('a', 1), ('B', 2), ('c', 3), ('D', 4), ('e', 5)]
# # both are same
# rdd=spark.sparkContext.parallelize(r1)
# rdd=sc.parallelize(r1)
# #print(rdd.sortByKey().collect())








