#.flatMap()
# 1. Change dept name in file
'''
deptno,deptname
011,HR
012,IT
013,Production
014,Infra
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark import* #(SparkContext)
sc=SparkContext.getOrCreate()
spark = SparkSession(sc)

dataschema1 = StructType([StructField("id", dataType=IntegerType()),
                              StructField("fname", dataType=StringType()),
                              StructField("lname", dataType=StringType()),
                              StructField("age", IntegerType()),
                              StructField("gender", StringType()),
                              StructField("deptno", IntegerType()),
                              StructField("salary", LongType())
                              ])
#df=spark.createDataFrame(data,schema)

df = spark.read.csv(
    r"C:\Users\KAJAL\PycharmProjects\spark\spark\KB.txt", schema=dataschema1)
#print(df)
from pyspark.sql.functions import *
df2 = df.withColumn("deptno", when(df.deptno == 11,"HR")
                                 .when(df.deptno == 12,"Infra")
                                 .when(df.deptno == 13,"Production")
                                  .when(df.deptno == 14,"IT")
                                 .otherwise(df.deptno))

#df2.show()

# Q-2 select average salary9 from department
#df.groupBy("deptno").sum("salary").show(truncate=False)
#df.groupBy("deptno").avg("salary").show(truncate=False)
#df.groupBy("deptno").max("salary").show(truncate=False)

#Q-3 provide employee details who has second highest salary
# df.groupBy("deptno").max("salary").alias("max_s").show(truncate=False)

#-------------------------Window function----------------
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
# win= Window.partitionBy("deptno").orderBy(col("salary").desc())
# df.select("deptno","salary").withColumn("row_number",row_number().over(win)).where(col("row_number")==1).show()
'''SQL'''
df.createOrReplaceTempView("table1")
# spark.ddl("SELECT  MAX(salary) AS salary FROM table1 WHERE salary IN(SELECT salary FROM table1 MINUS SELECT MAX(salary) FROM table1)").show()

'''2nd m'''# 2nd max
# spark.ddl("SELECT MAX(salary) AS salary FROM table1 WHERE salary <> (SELECT MAX(salary) FROM table1)").show()

'''3rd m'''# n number of salary
# n=int(input("plz enter the n value"))
# spark.ddl("select * from (select fname,id,deptno, salary, dense_rank() over(order by salary desc) r from table1) where r==2").show()







# By group by function



#df.salary.orderBy(desc).show(truncate=False)

#df.select(min("salary")).show(truncate=False)


#Q-4. retrieve employee_details with unique records from employee_detail.txt and employee_details1.txt and store them in different file.
df1=df.distinct()
#df1.write.option("header", True).csv(r"C:\Users\RITU\PycharmProjects\pythonProject\write")
#df1.show()

# Q-5. Create a dataframe from existing rdd/existing collection using various ways


columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
#1-parallelize

rdd=sc.parallelize(data)

#2-toDF()
dfFromRDD1 = rdd.toDF()

#dfFromRDD1.printSchema()
# or
dfFromRDD1 = rdd.toDF(columns)
#dfFromRDD1.printSchema()
# or
dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
#or
dfFromData2 = spark.createDataFrame(data).toDF(*columns)
#dfFromData2.show()
#or

#3- CSV file

df2 = spark.read.csv(r"C:\Users\KAJAL\PycharmProjects\spark\spark\KB.txt")

#s1 =min((df.map(lambda x:x.split(",")).map(lambda x:x[6]))).distinct().takeOrdered(2, key=lambda x:-x)
#s1= (df.map(lambda x:x.split(",")).filter(lambda x:int(x[6])== df2))
#print(s1.collect)


# df.withColumn("salary",col("salary").cast("Integer")).show()

# Q-6 1. retrive record which is having maximue date with respective of id, name column
simpleData = [(1,"Yesh","Patil",29,"M",11,20000),
              (2,"Ram","Wagh",30,"M",12,3000000),
              (3,"Sita","Patil",29,"F",11,50000),
              (4,"Kiran","XYZ",33,"F",11,40052),
              (5,"Savita","Waghmare",35,"F",12,800000)
              ]

schema = ["eid","first_name","last_nmae","age","sex","d_id","salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
# df.printSchema()
# df.show(truncate=False)
# df.groupBy("d_id").sum("salary").show(truncate=False)
# df.groupBy("d_id").avg("salary").show(truncate=False)
# df.groupBy("d_id").max("salary").show(truncate=False)
# df.groupBy("d_id","eid").max("salary").show(truncate=False)
# df.select('d_id','salary','first_name','eid').groupby("d_id","eid").max('salary').show()

"""window fun groupBy"""

from pyspark.sql.window import Window # for window function
from pyspark.sql.functions import * # for column
# #=================================================================
# windowSpec  = Window.partitionBy("d_id",'eid').orderBy("salary")
# df.withColumn("row_number",row_number().over(windowSpec)).where(col("row_number")==1).show()
#=================================================================
"""with column"""
# df.withColumn('max',row_number().over(windowSpec)).groupby("d_id","eid").agg(max('salary')).show()
# ==================================================================
"""SQL query"""
# df.createOrReplaceTempView("table")
# spark.ddl("select d_id, salary from table group by salary, d_id order by d_id ").show()

#-------------------2nd data ---------------------------------------
#1. retrive record which is having maximue date with respective of id, name column
schema1 = "id int,name string,price int,date string"
# df3 = spark.read.csv(path=r"C:\Users\erdus\PycharmProjects\pythonProject\project\date_assignment", schema=schema1)
# df3.show()
# df3.select('*').groupBy("id","name").agg(max("date")).alias("max_s").show(truncate=False)


# Q-7 expected output as below
# window= Window.partitionBy("id","name").orderBy("id")
# dd=df3.withColumn('next_date',lag("date").over(window))
# dd1=dd.withColumn('next_date',when(dd.next_date.isNull(),"2023-01-01").otherwise(dd.next_date))
# dd1.show()

# Q-9 get employee details, who has salary more than avg salary of all the employees

# spark.ddl("select * from table1 where salary < (select avg(salary) from table1)").show()

# Q-10 get count of an employee based on joining month

# mon= df3.select("id","name","price", col("date"),month(col("date")).alias("month"))
# mon.groupby("month").count().show() # 1st method
# df3.show()

# mon.createOrReplaceTempView("mon")
# spark.ddl("select month, count(month) from mon group by ,month").show() # 2nd method

# Q- 11 get a count of an employee based on joining year
# ye= df3.select("id","name","price", col("date"),year(col("date")).alias("year"))
# ye.groupby("year").count().show() # 1st method

# Q-12 get employee details with depart name who has joined in a leap year

# ye.select("id","name","date").where(col("year")%4==0).show()