#=======================================================
# pass this value with the help of config file
# [id]
# id = 20,22,25,30,35,38,46,60,72,78
#=========================================================
# import configparser
#
# # create cnfig object
# cofig = configparser.ConfigParser()
# cofig.read(r'C:\Users\Dell\PycharmProjects\spark\etl_project\config\cong.ini')
#
# id = cofig.get('column','id')
# print(id)
#
#
# from pyspark.sql.functions import col
#
#
# class Filter:
#
#
#
#     def id_filter(self,df,value,column):
#         '''
#
#         :param df: pass df
#         :param column: present column
#         :param value: id column values
#         :return: df
#         '''
#
#
#         if value!= ['']: # if column is empty
#             for i in value:
#
#                 df = df.withColumn(column,filter(col(column)==(i)))
#             return df
#
#         else:
#             return df
#

import findspark
from jsonschema._validators import contains

findspark.init()
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('dsa').getOrCreate()

df = spark.read.csv(r"D:\Brainwork\Cloud\PCM PROJECT\rawdata/data.csv")
df.show()

df = df.withColumn("quarantine_null_"+ "c1",
            when((df._c1.isNull()) |
            (trim(df._c1.cast("string")) == ""),
            struct(lit("1").alias("col_name"),df._c1.
             cast('string').alias("col_value"),lit("3").
                   alias("quarantine_reason"))).otherwise(lit(None)))
df.show()
df = df.withColumn('_c1'+"_original", col('_c1'))
df = df.withColumn('_c1', col('_c1').cast('string'))
#
df = df.withColumn("quarantine_cast_"+ 'c1',
                          when((((df._c1.isNotNull()) & (trim(df._c1
                          .cast("string"))!= "")) & ((df._c1_original.isNull()) |
                           (trim(df._c1_original.cast("string")) == ""))) |
                            (df._c1.isNull() & ((df._c1_original
                            .isNotNull()) & (trim(df._c1_original.cast("string")) != "")))
                            ,struct(lit('_c1').alias("col_name"),df._c1_original
                             .cast('string').alias("col_value"),concat(lit("xx "),lit('string')).
                              alias("quarantine_reason"))).otherwise(lit(None)))

df.show()
quar_columns = [column for column in df.columns if column.startswith("quarantine_")]
validated_df =df.withColumn("reason_array",array(quar_columns))
validated_df.show()
spark_dataframe = validated_df.withColumn("quarantine_information",array_except(col("reason_array"),
                array(struct(lit(None).cast("string").alias("col_name"),
                lit(None).cast("string").alias("col_value"),lit(None).cast("string").alias("quarantine_reason")))))
spark_dataframe.show()

valid_data = spark_dataframe.filter(size(col("quarantine_information")) > 1)
valid_data.show()
valid_data=valid_data.drop("quarantine_reason")
valid_data.show()
valid_data.select('quarantine_information').show()
from pyspark.sql import Row
#
# spark_dataframe = validated_df.withColumn(("xxxxxxx xxxxxxx"),array_except(col("reason_array"),
#                 array(struct(lit(None).cast("string").alias("col_name"),
#                 lit("h").cast("string").alias("col_value"),lit(None).cast("string").alias("quarantine_reason")))))
# spark_dataframe.show()

#
avro_schema = spark.read.json(r"C:\Users\Dell\PycharmProjects\cloud\oozie\pyspark_script-1\3\schema.json", multiLine=True)
avro_schema.show()
avsc_record_name = avro_schema.select("name").collect()[0].name
print(avsc_record_name)
avro_schema = avro_schema.withColumn("fields", explode(col("fields")))
avro_schema.show()
fieldsToCheck = avro_schema.select("fields.*")
fieldsToCheck.show()
fieldsToCheck = fieldsToCheck.withColumn("isNullable", col("type").contains("null"))
fieldsToCheck.show()
localFieldList = fieldsToCheck.collect()
print(localFieldList)

for field in localFieldList:
    print(field)
    typeFound = None
    if '''"type":"long"''' in field.type or ('''"type":"array"''' not in field.type and "long" in field.type):
        typeFound = "long"
    if '''"type":"string"''' in field.type or ('''"type":"array"''' not in field.type and "string" in field.type):
        typeFound = "string"
    if '''"type":"boolean"''' in field.type or ('''"type":"array"''' not in field.type and "boolean" in field.type):
        typeFound = "boolean"
    if '''"type":"int"''' in field.type or ('''"type":"array"''' not in field.type and "int" in field.type):
        typeFound = "int"
    if '''"type":"double"''' in field.type or ('''"type":"array"''' not in field.type and "double" in field.type):
        typeFound = "double"
    if '''"logicalType":"timestamp-millis"''' in field.type:
        typeFound = "timestamp"
    if not field["isNullable"]:  # all fields check
        print(typeFound)
host
src
_event_origin
_event_tags
carrier
device_uid
enodeb_id
sector

