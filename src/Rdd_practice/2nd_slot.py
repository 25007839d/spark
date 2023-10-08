# import findspark
# findspark.init()
# from pyspark.sql.types import StructField
#
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import*
# import re,os
#
# if __name__=='__main__':
#     spark = SparkSession.builder.master('local[*]').getOrCreate()



    # schema = StructType([
    #     StructField('name', StructType([
    #         StructField('firstname', StringType(), True),
    #         StructField('middlename', StringType(), True)
    #
    #     ])),
    #     StructField('state', StringType(), True)
    #
    # ])
    #
    # df = spark.read.csv(r"C:\Users\Dell\Desktop\table\nested.csv", header=True, schema=schema)
    # df.show()


    # rdd = spark.sparkContext.textFile(r"C:\Users\Dell\Desktop\rdd1.csv")
    # def sp(x):
    #
    #     s=''
    #     for i in x:
    #       for j in i:
    #         if j in ['/','-','_']:
    #             s=s+','
    #         else:
    #             s=s+j
    #     return s
    # rdd1= rdd.map(lambda x:sp(x).split(','))
    #
    # print(rdd1.collect())
    #
    # header = rdd1.first()  # extract header
    # print(header)
    # frdd=rdd1.filter(lambda x:x!=header)
    #
    # df = frdd.toDF(['id','f_name','l_name','salary'])
    # df.show()

    #----------2nd approach--------------

  #   nsd=spark.read.csv(r"C:\Users\Dell\Desktop\rdd1.csv",header=True)
  #   # nsd.show()
  # #
  #
  #   sdf= nsd.withColumn("lname", translate(col('lname'), '-/_', ',,,'))
  #   sdf1=sdf.select('id','fname',split(col("lname"),",").alias("raw"))
  #   sdf1.withColumn("lname",col("raw")[0]).withColumn("salary",col("raw")[1]).drop("raw").show()


#---------create 10. I/p--pallavi


    # df=spark.read.csv(r"C:\Users\Dell\Desktop\table\df.csv",header=True)
    #
    # df1=df.withColumn("adds",concat_ws(",","add1","add2")).withColumn("adds",split("adds",","))
    #
    # from pyspark.ddl.functions import explode
    # df1.select("id","name",explode(df1.adds).alias("adds")).filter(col("adds") != "null").show()

    # p = os.getcwd()
    # print(p)

#------------------struct field and nested schema
# star pattern
a='    '
count=0
for i in range(5):

    for j in range(1):
        a+='*'
        print(a)
        for i in a:
            if i ==' ' and count<4:
                cpunt=1
            else:



