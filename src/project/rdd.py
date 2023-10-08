from pyspark.context import SparkContext
from pyspark.conf import SparkConf
conf = SparkConf().setAppName('rdd').setMaster('local[*]')
sc = SparkContext(conf=conf)
rdd = sc.parallelize([1,4,6])
print(rdd.collect())
