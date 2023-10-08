import findspark
findspark.init()
import configparser
from pyspark.sql.types import StructField, StringType, DateType, TimestampType, DecimalType,StructType
from pyspark.sql import *


#spark session create with jdbc jar config

spark = SparkSession.builder.appName('oracle_connection')\
        .config('spark.jars',r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\ojdbc8.jar")\
        .getOrCreate()




#getting whole table data

class Table_sql:   # create SQL table read class method

        def tbl(self,driver,url,user,table8,password):

            t_m_migration =spark.read.format('jdbc')\
                    .option('driver',driver)\
                    .option('url',url)\
                    .option('user',user)\
                    .option('dbtable',table8)\
                    .option('password',password)\
                .load()




            return t_m_migration


