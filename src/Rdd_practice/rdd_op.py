import regexp as regexp
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
import re

if __name__=='__main__':
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    df=spark.read.csv(r"D:\Brainwork\Cloud\SCD PROJECT\Dushyant\Final_target_3\f.csv",header=True)
    # df.show()
#     print(df.schema)




    from pyspark.sql.functions import expr

    def d(a):
        print(a)
        a1=''
        a2=''
        a3=''
        count=0
        for i in a:
            if i !='-' and count==0:
                a1=a1+i
            elif i =="-" :
                count=count+1

            elif i !='-' and count==1:
                a2=a2+i

            elif i !="-" and count==2:
                a3=a3+i
        li = [a1, a2, a3]
        day=''
        mon=''
        year=''

        for i in li:

            if  len(i)==2 and int(i)<13 and mon=='':
                mon +=i
            elif len(i)== 4:
                year+=i
            elif len(i)==2 and i != [mon,year]:
                day=day+i

        date=year+'-'+mon+'-'+day

        return date


    dateformat = udf(lambda x: d(x))
    dformat = ['dd.MM.yyyy']
    # df1 = df.withColumn("date_type", to_date(("date"), 'dd-MM-yyyy'))
    # df2 = df.withColumn('edate', (regexp_replace('date', '[99U^%-/]', '-')))
    # def to_date(link,Format=('dd-MM-yyyy','dd-yyyy-MM')):
    #     return coalesce([to_date(link,f) for f in Format])
    # nd=udf(lambda x:to_date(x))
    # ndf=df2.withColumn('edate',nd(col('edate')))


    # df3 = df2.withColumn('edate', (to_date(dateformat(col('edate')))))
    #
    # df3.show()
    # ndf.show()




    # rdd2 = rdd.flatMap(lambda x: x.split(" "))
    # print(rdd2.collect())

    # def call(x):
    #     print(x)
    #     return x
    #
    #
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



    # a=['HIVE','spark']

    # rdd3 = rdd2.map(lambda x: (x[0],x[1]))
    #
    # rdd4= rdd3.filter(lambda x: a in x)
    # print(rdd3.collect())

    # rdd6= rdd5.repartition(1)

    # rdd6.saveAsTextFile(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\int")
    #
    # rdd5 = rdd4.map(lambda x: (x[0]))
    # # Print rdd6 result to console
    # print(rdd5.collect())
    #
    # rdd6 = rdd5.filter(lambda x: 'is' not in x)
    # print(rdd6.collect())

    # lis = [[1, 2,4,3], [4, 5, 6, 4], [8, 6, 4, 8, 2]]
    # c = []
    # nlis = []
    #
    # for i in lis:
    #     c.append(i[0])
    #
    # le = len(c)
    # for i in lis:
    #     i[0] = c[le - 1]
    #     le = le - 1
    #     print(i)
    # le1= len(lis)
    # cc=[]
    # for i in lis:
    #     cc.append(lis[le1-1])
    #     le1=le1-1
    # print(cc)


    # df.show()

    ndf = spark.read.csv(r'C:\Users\Dell\Desktop\table\tab.csv',header=True)

    ndf.show()
    ndf.select("c1","c2",regexp_replace('c2','\\|',"/").alias('r')).show()
    ndf1= ndf.select("c1",split(col("c2"),"\\|",-1).alias("split_st"))
    ndf1.show()
    ndf1.select("c1",explode(ndf1.split_st)).show()
    ndf.where(col('c1')=='1').show()
    ndf.distinct().show()
    ndf.drop_duplicates('c1').show()




