import findspark
findspark.init()

import configparser
from pyspark.sql import *
from etl_project.utils.schema import Schema
from pyspark.sql.types import StructField, StringType, DateType, TimestampType, DecimalType,StructType
from etl_project.ddl.sql_table import Table_sql
from etl_project.utils.valadition import Filter


if __name__=='__main__':
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName('Optimize BigQuery Storage') \
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.15.1-beta') \
        .config('spark.jars', r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\ojdbc8.jar") \
        .config('spark.driver.bindAddress','localhost')\
        .config('spark.ui.port','4040')\
        .getOrCreate()

    # create cnfig object
    cofig = configparser.ConfigParser()
    cofig.read(r'C:\Users\Dell\PycharmProjects\spark\etl_project\config\cong.ini')



    # pass variable through config
    driver = cofig.get('sql', 'driver')
    url = cofig.get('sql', 'url')  # connection host, port
    user = cofig.get('sql', 'user')
    password = cofig.get('sql', 'password')
    table8 = cofig.get('sql', 'table8')

# filter column
    state = cofig.get('column','state')
    table_id = cofig.get('column','table_id')

# pass secquance name
    id_col_name = cofig.get('secquance','id_col_name')

# pass rename column p_column-n_column
    p_column = cofig.get('rename','p_column').split(',')
    n_column = cofig.get('rename','n_column').split(',')

# pass split column and seprator
    column   = cofig.get('split','column').split(',')
    seprator = cofig.get('split','seprator').split(',')

# pass explode column
    e_column = cofig.get('explode','e_column').split(',')

# pass rtrim column and value
    l_column = cofig.get('lpad','l_column')
    count    = cofig.get('lpad','count')
    value    = cofig.get('lpad','value')


# pass replace column and value
    r_column  = cofig.get('replace','r_column')
    c_value   = cofig.get('replace','c_value')
    r_value   = cofig.get('replace','r_value')

    # call dataframe call



    tbl_obj = Table_sql().tbl(driver,url,user,table8,password)
    # tbl_obj.show()

    # tbl_obj.repartition(1).write.option("header",True).csv(r'C:\Users\Dell\OneDrive\Desktop\datta1.csv')
    # state filter df-state NJ

    df_NJ = Filter().state(tbl_obj,state)
    # df_NJ.show()
    # print(df_NJ.schema)

    # Table filter - table_id 85.(lc)

    df_NJ_85  = Filter().table(df_NJ,table_id)
    # df_NJ_85.show()

    #create secquancing
    df_NJ_85_id = Filter().id_no(df_NJ_85,id_col_name=id_col_name)
    # df_NJ_85_id.show()

    # column rename
    df_NJ_85_id_re = Filter().rename(df_NJ_85_id,p_column,n_column)
    # df_NJ_85_id_re.show(500)

    # split column
    df_NJ_85_id_re_sp = Filter().split(df_NJ_85_id_re,column,seprator)
    # df_NJ_85_id_re_sp.show(500)

    # explode column
    df_NJ_85_id_re_sp_ex = Filter().explod(df_NJ_85_id_re_sp,e_column)
    # df_NJ_85_id_re_sp_ex.show()

    # lpad column and value
    df_NJ_85_id_re_sp_ex_lp = Filter().lpad(df_NJ_85_id_re_sp_ex,l_column,count,value)
    # df_NJ_85_id_re_sp_ex_lp.show()

    # replace column value
    df_NJ_85_id_re_sp_ex_lp_rp = Filter().replace(df_NJ_85_id_re_sp_ex_lp,r_column,c_value,r_value)
    df_NJ_85_id_re_sp_ex_lp_rp.show()
    print(df_NJ_85_id_re_sp_ex_lp_rp.rdd.getNumPartitions())
    # count verification of state column
    print(df_NJ_85_id_re_sp_ex_lp_rp.select('state').count())
    # print(df_NJ_85_id_re_sp_ex_lp_rp.rdd.partitions.length)
    # print(df_NJ_85_id_re_sp_ex_lp_rp.rdd.partitions.size)
    # load data to bq
    # from etl_project.ddl.bq_loader import load_BQ
    # load_BQ(df_NJ_85_id_re_sp_ex_lp_rp)

    input("spark close input")
    spark.stop()





