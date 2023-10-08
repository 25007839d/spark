import regexp as regexp
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
import re

if __name__=='__main__':
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    rdd= spark.sparkContext.textFile(r'C:\Users\Dell\Desktop\rdd.txt')
    print(rdd.collect())
    rddsplit=rdd.map(lambda x: x.split(',')).map(lambda x:int(x[2])).max()
    print(rddsplit)




    def third_hightest(list1):
        print(list1)
        y = list(list1)
        print(y)
        return y[-3]


    # def max(x):
    #     print(x)
    #     a=0
    #     b=0
    #     for i in x:
    #         if int(i)>a:
    #             b=a
    #             a=int(i)
    #     return  int(a)

    # header = emp_det_rdd.map(lambda x: x.split(",")).collect()[0]
    # print(header)

    rdd_sal1 = rdd.map(lambda x: x.split(","))\
        .map(lambda x: (1, int(x[2]))).sortBy(lambda x:x[1]).groupByKey().mapValues(third_hightest).map(lambda x: (x[1],x[0]))
    print(rdd_sal1.collect())

    rdd_n = rdd.map(lambda x: x.split(",")).map(lambda x: (int(x[2]), x))
    print(rdd_n.collect())
    #
    rdd_join = rdd_sal1.join(rdd_n)
    print(rdd_join.collect()[0][1][1])

    nsd = spark.read.csv(r"C:\Users\Dell\Desktop\rdd1.csv", header=True)
    nsd.show()

