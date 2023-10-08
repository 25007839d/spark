#There are three ways to create an RDD in Spark.

#1.Parallelizing already existing collection in driver program.
#2.Referencing a dataset in an external storage system (e.g. HDFS, Hbase, shared file system).
#3.Creating RDD from already existing RDDs.

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("First RDD").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # RDDs -  resilient distributed dataset
    # Parallelized Collections:
    data = [1, 2, 3, 4]
    parallelized_rdd = sc.parallelize(data)
    print("===== Parallelized RDD =========")
    print(parallelized_rdd.collect())
    print(parallelized_rdd.getNumPartitions())
    print("================================")

    # Existing Dataset RDD
    line = sc.textFile("C:\\Users\\tadit\\PycharmProjects\\sparklearning\\RDD\\input_test.txt")
    print("===== Existing Dataset RDD =========")
    print(line.collect())
    print(line.getNumPartitions())
    print("================================")

# Accumulator variables

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .appName("Broadcast variable") \
        .getOrCreate()

    accvar = spark.sparkContext.accumulator(1)  # declare

    rdd = spark.sparkContext.parallelize([1, 2, 3])


    def func(x):
        accvar.add(x)

        """
        accvar = 1
        2
        4
        7
        """


    rdd.foreach(func)
    print("acc value: " + str(accvar.value))

from pyspark.sql import SparkSession

# broadcast_variable:  Readonly variable
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .appName("Broadcast Variable Example") \
        .getOrCreate()

    states = {"MH": "Maharashtra", "MP": "Madhyapradesh", "GJ": "Gujarat"}
    broadcaststate = spark.sparkContext.broadcast(states)

    inputdatardd = spark.sparkContext.textFile("input_data/employee_data.txt")
    print(inputdatardd.getNumPartitions())

    def changestateval(x):
        # 101,Aditya,Tambe,India,MH
        xsplit = x.split(",")  # list [101,Aditya,Tambe,India,MH]
        testlist = []
        for i in xsplit:
            try:
                i = broadcaststate.value[i]
            except:
                pass
            testlist.append(i)
        return ','.join(testlist) # string comma

    print(inputdatardd.map(lambda x: changestateval(x)).collect())

# data frame
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .appName("Dataframe Intro") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.ui.port", "4050") \
        .getOrCreate()

    inputdata = Row(1, 2, 3)
    inputrdd = spark.sparkContext.parallelize(inputdata)

    # create dataframe from rdd
    inputdf = inputrdd.map(lambda x: (x,)).toDF()
    inputdf.printSchema()
    #inputdf.show()

    inputdata1 = [("Java", "80"), ("Math", "75")]
    inputrdd1 = spark.sparkContext.parallelize(inputdata1)
    inputdf1 = inputrdd1.toDF(["subject", "mark"])
    # inputdf1.show()
    # inputdf1.printSchema()

    # create a dataframe using createDataframe()
    # dataframe from rdd
    inputdf2 = spark.createDataFrame(inputrdd1).toDF(*["subject", "mark"])
    # inputdf2.show()

    # dataframe from inputdata instead of rdd
    inputdf3 = spark.createDataFrame(inputdata1).toDF(*["subject", "mark"])
    # inputdf3.show()

    # dataframe from row type
    inputdata2 = [Row("Java", "80"), Row("Math", "75")]
    inputdf4 = spark.createDataFrame(inputdata2, ["subject", "mark"])
    # inputdf4.show()

    # create a dataframe with schema
    inputdata5 = [("Aditya", "IT", "Pune", 2000),
                  ("Ram", "HR", "Mumbai", 1500),
                  ("Shyam", "IT", "Pune", 9000)]

    schema5 = StructType([StructField("name", StringType(), True),
                          StructField("dept_name", StringType(), True),
                          StructField("city", StringType(), True),
                          StructField("salary", IntegerType(), True)])

    df5 = spark.createDataFrame(data=inputdata5, schema=schema5)
    # df5.printSchema()
    # df5.show()

    # create dataframe from datasource
    # create dataframe from csv file
    csvdf = spark.read.csv(path="C:\\Users\\tadit\\PycharmProjects\\sparklearning\\practice\\input_data\\zipcode_withoutheader.csv")
    # csvdf.printSchema()
    # csvdf.show()

    csvdf1 = spark.read.csv(path="C:\\Users\\tadit\\PycharmProjects\\sparklearning\\practice\\input_data\\employee.csv",schema=schema5)
    # csvdf1.show()
    # csvdf1.printSchema()
    #
    # input("Press enter to terminate")
    # spark.stop()
© 2022 GitHub, Inc.
Terms

# pyspark RDD
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    sparkconf = SparkConf().setAppName("PYSPARK_RDD").setMaster("local[*]")
    sc = SparkContext(conf=sparkconf)
    # print(sc)

    # creating Spark Session
    spark = SparkSession.builder.master("").appName("Python Spark RDD basic example").getOrCreate()
    # print(spark)

    # RDD (Resilient Distributed Datasets)
    # Parallized Collection:
    inputdata = [1, 2, 3, 4, 5]
    inputrdd = sc.parallelize(inputdata)
    # print(inputrdd.collect())
    sparkinputrdd = spark.sparkContext.parallelize(inputdata)
    # print(sparkinputrdd.collect())

    # External Datasets
    inputrdd1 = sc.textFile("input_data/data.txt")
    # print(inputrdd1.collect())
    sparkinputrdd = spark.sparkContext.textFile("input_data/data.txt")
    # print(sparkinputrdd.collect())

    # RDD using wholeTextFile
    inputrdd = sc.wholeTextFiles("input_data/")
    # print(inputrdd.collect())

    sparkinputrdd = spark.sparkContext.wholeTextFiles("input_data/")
    # print(sparkinputrdd.collect())

    # empty RDD
    emptyrdd = spark.sparkContext.emptyRDD
    # print(emptyrdd.collect())

    # get number of partition
    # print("sparkinputrdd num partition : " + str(sparkinputrdd.getNumPartitions()))
    # print("inputrdd1 num partition : " + str(inputrdd1.getNumPartitions()))
    ## print("emptyrdd num partition : " + str(emptyrdd.getNumPartitions()))

    # repartition and coalesce
    # repartition() method which shuffles data from all nodes also called full shuffle and
    # second coalesce() method which shuffle data from minimum nodes
    # Note: repartition() or coalesce() methods also returns a new RDD.

    reparsparkinputrdd = sparkinputrdd.repartition(4)
    # print("after repartition : sparkinputrdd num partition : " + str(reparsparkinputrdd.getNumPartitions()))

    # RDD Transformation
    # flatMap(): Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.
    rddflatmap = inputrdd1.flatMap(lambda x: x.split(" "))
    # print(rddflatmap.collect())

    # map(): transformation is used the apply any complex operations like adding a column, updating a column e.t.c,
    # the output of map transformations would always have the same number of records as input.
    rddmap = rddflatmap.map(lambda x: (x, 1))
    # print(rddmap.collect())

    # reduceByKey(): merges the values for each key with the function specified.
    rddreducebykey = rddmap.reduceByKey(lambda x, y: x + y)
    # print(rddreducebykey.collect())

    # sortByKey(): sort RDD elements based on key
    rddsortbykey = rddreducebykey.map(lambda x: (x[1], x[0])).sortByKey()
    # print(rddsortbykey.collect())
    # print(rddreducebykey.sortByKey().collect())

    # filter()
    filterrdd = rddsortbykey.filter(lambda x: 'o' in x[1])
    # print(filterrdd.collect())

    # union():
    unionrdd = rddmap.union(rddreducebykey)
    # print(unionrdd.collect())

    # intersection(): Returns the dataset which contains elements in both source dataset and an argument
    intersecrdd = unionrdd.intersection(rddsortbykey)
    # print(intersecrdd.collect())

    # distinct()
    distinctrdd = rddflatmap.distinct()
    # print(rddflatmap.collect())
    # print(distinctrdd.collect())

    # cache(): Persist this RDD with the default storage level (MEMORY_ONLY).
    # print(distinctrdd.cache().collect())

    # persist(): is used to store the RDD to one of the storage levels
    # MEMORY_ONLY,MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY,
    # MEMORY_ONLY_2,MEMORY_AND_DISK_2
    import pyspark
    perdistrdd = intersecrdd.persist(pyspark.StorageLevel.MEMORY_ONLY)
    # print(perdistrdd.collect())
    perdistrdd1 = perdistrdd.unpersist()

    # cogroup(): For each key k in self or other,
    # return a resulting RDD that contains a tuple with the list of values for that key in self as well as other.
    x = sc.parallelize([("a", 1), ("b", 4)])
    y = sc.parallelize([("a", 2)])
    # print([(x, tuple(map(list, y))) for x, y in sorted(list(x.cogroup(y).collect()))])

    # def test(rdd1, rdd2):
    #     for a, b in rdd1.cogroup(rdd2).collect():
    #         print(a)
    #         print(b)
    #         print(tuple(map(list, b)))
    #
    # test(x, y)

    # groupByKey(): Group the values for each key in the RDD into a single sequence.
    # Hash-partitions the resulting RDD with numPartitions partitions
    print("================GroupByKey========================")
    print(rddmap.groupByKey().mapValues(list).collect())
    print("===================================================")

    # join
    x = spark.sparkContext.parallelize([("a", 1), ("b", 4)])
    y = spark.sparkContext.parallelize([("a", 2), ("a", 3)])
    print("======================Join========================")
    #print(sorted(x.join(y).collect()))
    print("==================================================")

    # RDD Action:
    # count(): Returns the number of records in an RDD
    #print("count: " + str(rddsortbykey.count()))

    # first()
    #print("first element in RDD: " + str(rddsortbykey.first()))

    # max(): Return max record
    #print("max record : " + str(rddsortbykey.max()))

    # reduce(): Reduces the records to single, we can use this to count or sum
    print(rddsortbykey.reduce(lambda a, b: (a[0] + b[0], a[1])))
    from operator import add
    print(spark.sparkContext.parallelize([1, 2, 3, 4, 5]).reduce(add))

    # take(): Returns the record specified as an argument.
    print(rddsortbykey.collect())
    print(rddsortbykey.take(2))

    # please refer broadcast_variable.py