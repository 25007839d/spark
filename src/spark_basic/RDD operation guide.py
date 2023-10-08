from pyspark.sql import SparkSession
from pyspark.sql.types import*
from pyspark import *
sc=SparkContext.getOrCreate()
spark = SparkSession(sc)

# rddf creation
collect_rdd = sc.parallelize([1,2,3,4,5])
#print(collect_rdd.collect())
#print(collect_rdd) # it will show only rdd address

#T2.he .count() Action

count_rdd = sc.parallelize([1,2,3,4,5,5,6,7,8,9])
#print(count_rdd.sum())  # we can use aggrigate function

#3. The .first() Action

first_rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
#print(first_rdd.first()) # onlly index 1st value

#4. The .take() Action

#take_rdd = sc.parallelize([1,2,3,4,5])
#print(first_rdd.take(3))

#5. The .reduce() Action

#print(first_rdd.reduce(lambda x, y : x + y)) # will give total sum of elements

#6. The .saveAsTextFile() Action

#first_rdd.saveAsTextFile('file.txt')# for write your work in file

# all above command are action command will see the transformation command

#1. The .map() Transformation

my_rdd = sc.parallelize([1,2,3,4])
#r=my_rdd.map(lambda x: x+ 10)
#print(r.max())

#2. The .filter() Transformation

filter_rdd = sc.parallelize([2, 3, 4, 5, 6, 7])
#print(filter_rdd.filter(lambda x: x%2 == 0).collect()) # it is collect the condition satisfied data
filter_rdd_2 = sc.parallelize(['Rahul', 'Swati', 'Rohan', 'Shreya', 'Priya'])
#print(filter_rdd_2.filter(lambda x: x.startswith('R')).collect())

#3. The .union() Transformation

union_inp = sc.parallelize([2,4,5,6,7,8,9])
union_rdd_1 = union_inp.filter(lambda x: x % 2 == 0)
union_rdd_2 = union_inp.filter(lambda x: x % 3 == 0)
union_rdd_3 = union_inp.filter(lambda x: x % 4 == 0)
#print(union_rdd_1.union(union_rdd_2).union(union_rdd_3).collect())

#4. The .flatMap() Transformation

flatmap_rdd = sc.parallelize(["Hey there", "This is PySpark RDD Transformations"])
#print(flatmap_rdd.flatMap(lambda x: x.split(" ")).collect())

#PySpark Pair RDD Operations
marks = [('Rahul', 88), ('Swati', 92), ('Shreya', 83), ('Abhay', 93), ('Rohan', 78)]
d1=sc.parallelize(marks)
#print(d1.collect())

#1. The .reduceByKey() Transformation
marks_rdd = sc.parallelize([('Rahul', 25), ('Swati', 26), ('Shreya', 22), ('Abhay', 29), ('Rohan', 22), ('Rahul', 23), ('Swati', 19), ('Shreya', 28), ('Abhay', 26), ('Rohan', 22)])
#print(marks_rdd.reduceByKey(lambda x, y: x + y).collect()) # it removed the matched key only select one

#2. The .sortByKey() Transformation

marks_rdd = sc.parallelize([('Rahul', 25), ('Swati', 26), ('Shreya', 22), ('Abhay', 29), ('Rohan', 22), ('Rahul', 23), ('Swati', 19), ('Shreya', 28), ('Abhay', 26), ('Rohan', 22)])
#print(marks_rdd.sortByKey('ascending').collect())

#3. The .groupByKey() Transformation
marks_rdd = sc.parallelize([('Rahul', 25), ('Swati', 26), ('Shreya', 22), ('Abhay', 29), ('Rohan', 22), ('Rahul', 23), ('Swati', 19), ('Shreya', 28), ('Abhay', 26), ('Rohan', 22)])
dict_rdd = marks_rdd.groupByKey().collect()
for key, value in dict_rdd:
    #print(key, list(value))

#Actions in Pair RDDs
#1. The countByKey() Action
    marks_rdd = sc.parallelize(
        [('Rahul', 25), ('Swati', 26), ('Rohan', 22), ('Rahul', 23), ('Swati', 19), ('Shreya', 28), ('Abhay', 26),
         ('Rohan', 22)])
    dict_rdd = marks_rdd.countByKey().items()
    for key, value in dict_rdd:
    #print(key, value)

