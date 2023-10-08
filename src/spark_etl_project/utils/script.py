import findspark
findspark.init()
from pyspark.sql.functions import monotonically_increasing_id, col, lpad, split, when, explode_outer

from pyspark.sql import *

spark = SparkSession.builder.appName('oracle_connection')\
        .config('spark.jars',r"../ddl/ojdbc8.jar")\
        .getOrCreate()
from etl_project.ddl.sql_table import tbl

hr = Sql().tbl() # call to table class
hr.show() # create df , call class function
# t_m_migrate.show()

df1=hr.filter(( col("table_number")=="85.(LC)") & (col("state")=="NJ"))
# df1.show()
df2=df1.withColumn("id",monotonically_increasing_id()+1001)
# df2.show()
# print(df1.select("id").count())




df3=df2.withColumn("symbol",explode_outer(split("key3","&")))\
       .withColumn('symbol',when(col('symbol')=='AA','A').when(col('symbol')=='AB','A').otherwise(col('symbol')))\
       .withColumn("construction_code",explode_outer(split("key4","or")))\
       .withColumn("class_code",lpad("key1",4,"0"))\
       .withColumnRenamed("key2","coverage")
df3.show()

print(df3.select("id").count())


####2nd table####

df72=df.filter(( col("table_number")=="72.E.2.c.(2)(LC)") & (col("state")=="NJ"))
# df72.show()
df73=df72.withColumn("id",monotonically_increasing_id()+1001)
df74=df73.withColumnRenamed("key2","coverage")
# df74.show()

df75=df74.select("id","country","state","table_number","common_state","effective_date","expiration_date","coverage","factor")
# df75.show()
# print(df74.select("id").count())


#########3rd table#########

b72=df.filter(( col("table_number")=="72.E.2.b.(1)(LC)") & (col("state")=="NJ"))
b73=b72.withColumn("id",monotonically_increasing_id()+1001)
# b73.show()
# print(b73.select("id").count())

b74=b73.select("id","country","state","table_number","common_state","effective_date","expiration_date","factor")
# b74.show()
# print(b74.select("id").count())



#########4th table #######################


e70=df.filter(( col("table_number")=="70.E.2.e.BGII(LC)") & (col("state")=="NJ"))
# e70.show()
e71=e70.withColumn("id",monotonically_increasing_id()+1001)


e72=e71.withColumn("symbol",explode_outer(split("key3","&")))\
        .withColumnRenamed("key2","coverage")

e73=e72.select("id","country","state","table_number","common_state","effective_date","expiration_date","coverage","symbol","factor")
# e73.show()
print(e73.select("id").count())
#
# '''
# shal1 : to generate secquance by # kay
# crc32 : to generate random number --generate 100 k duplicate
# md5 : md5(col(emp).cast("string")))---50 M after duplicate /not suggested
# sha2: sha2(col(emp).cast("string"),512))  -to generate value if record 5 milion
#
# '''
# #auto generate id column value
# id_df = t_m_migrate.withColumn('id',monotonically_increasing_id()+1001)
# # id_df.show()
#
#
#
#
# # key-1 (class_code) length should be 4 by adding 0 before
# '''
# lpad : lpad('key1',4,'0')
# format_string :concatenation >> format_string('%04d','key')
#                or we can add string >> format_string('%s#%03d','country','key')
# concat : concat(lit('0000'))
# substring: substring('class_code',-4,4)
#
# '''
# nj_cc_df = id_df.withColumn("class_code",lpad('key1',4,'0'))
# # nj_cc_df.show()
#
#
# #key4 (construction_code) 5&6 >>5 replace
# '''
# single condition : regexp_replace
# multiple condition : regexp_replace with when
# '''
# nj_cc_cn_df= nj_cc_df.withColumn('construction_code',explode_outer(split('key4','or')))
#
# nj_cc_cn_df.show(5000)
#
#
# #kay3 (symbol) b&c >>b replace
# nj_cc_cn_sm_df = nj_cc_cn_df.withColumnRenamed("key3","symbol").withColumn('symbol',when(col('symbol')=='AA','A').
#                 when(col('symbol')=='AB','A').otherwise(col('symbol')))
#
# d=nj_cc_cn_sm_df.withColumn("symbol",explode_outer(split("symbol",'or')))
#
# print(nj_cc_cn_sm_df.select(col('state')).count())
# print(d.select(col('state')).count())
# nj_cc_cn_sm_df.show()
#
# # key2 (coverage) renamename
# nj_cc_cn_sy_co_df=nj_cc_cn_sm_df.withColumnRenamed("key2","coverage")
# # nj_cc_cn_sy_co_df.show()
#
#
# # final column select -------
# nj_df = nj_cc_cn_sy_co_df.select('id','country','state','table_number','common_state','effective_date','EXPIRATION_DATE'\
#                              ,'class_code','coverage','symbol','construction_code','factor' ).filter(col('state')=='NJ')
# # nj_df.show()
# print(nj_df.select(col('state')).count())
#
# #filter table 85.(LC)
# lc_85 = nj_df.select('id','country','state','table_number','common_state','effective_date','EXPIRATION_DATE'\
#                              ,'class_code','coverage','symbol','construction_code','factor' )\
#                                 .filter(col('table_number')=='85.(LC)')
# a=lc_85.select(col('state')).count()
# # lc_85.show()
# print(a)
#
#
# #filter table 85.TerrMult(LC)
# lc_T_85df = nj_df.select('id','country','state','table_number','common_state','effective_date','EXPIRATION_DATE'\
#                              ,'class_code','coverage','symbol','construction_code','factor' )\
#                               .filter(col('table_number') == '85.TerrMult(LC)')
# # lc_T_85df.show()
# b = lc_T_85df.select(col('state')).count()
# print(b)
#
# #filter table 72.E.2.c.(2)(LC)
# lc_2_72_df = nj_df.select('id','country','state','table_number','common_state','effective_date','EXPIRATION_DATE' \
#                                     , 'coverage', 'factor').filter(col('table_number') == '72.E.2.c.(2)(LC)')
#
# c=lc_2_72_df.select(col('state')).count()
# print(c)
#
# #filter table 72.E.2.c.(1)(LC)
# lc_1_72_df = nj_df.select('id','country','state','table_number','common_state','effective_date','EXPIRATION_DATE' \
#
#                                     ,'factor').filter(col('table_number') == '72.E.2.b.(1)(LC)')
#
# # lc_1_72_df.show()
# d= lc_1_72_df.select(col('state')).count()
# print(d)
#
# #filter table 70.E.2.e.BGII(LC)
# lc_70_df = nj_df.select('id','country','state','table_number','common_state','effective_date','EXPIRATION_DATE'\
#                             ,'coverage','symbol','factor' )\
#                                 .filter(col('table_number')=='70.E.2.e.BGII(LC)')
#
# e= lc_70_df.select(col('state')).count()
# print(e)
#
#
# # bigquery loading table-----------------------------------------------------------------------------------
#
# # spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
# #                                          r"C:\Big Data\ritu-351906-27e10a6678af.json")
# # lc_T_85df.write.format('bigquery') \
# #           .option('table', 'ritu-351906:bwt_session.lc_T_85') \
# #           .mode('append').save()
#
#
# #------------------check the condition ------------------------------
# NJ_STATE_COUNT = t_m_migrate.filter(col('state')=='NJ').select(col('state')).count()
# total_table_record = a+b+c+d+e
# if NJ_STATE_COUNT == total_table_record:
#     print('data process successfulley in spark')
# else:
#     print('NJ_STATE_COUNT ,{},{} total_table_record'.format(NJ_STATE_COUNT,total_table_record))
#


























































































































































































































































































































































































































































































