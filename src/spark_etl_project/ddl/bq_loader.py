
import findspark
from google.cloud.storage import blob
from google.cloud import bigquery
findspark.init()
from pyspark.sql import SparkSession
from google.cloud import storage
import os
os.environ["GCLOUD_PROJECT"]="formal-purpose-352005"
credentals_path =r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\formal-purpose-352005-d58e2dcced94.json"
# as connector
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentals_path


spark = SparkSession.builder \
  .appName('Optimize BigQuery Storage') \
  .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.15.1-beta') \
  .getOrCreate()



def load_BQ(df):
            # '''
            #
            # :param gdf: which data frame we want to load on bq
            # :return: no return
            # we can do all soft coading of gcp, gcs,bq lnk
            # '''

        spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                         r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\formal-purpose-352005-d58e2dcced94.json")

        # spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")

        bucket = "etl-d-r"
        spark.conf.set('temporaryGcsBucket', bucket)


        # df.write.format('bigquery') \
        # .option('table', 'ritu-351906:bwt_session.try5') \
        # .save()
        table ='tbl_85'

        # through jar file
        df.write.format('bigquery') \
                .option('table', 'etl.tbl_85') \
                .mode('append')\
               .save()

        # df.write.option("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"). \
        #         option("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"). \
        #         option("google.cloud.auth.service.account.enable", "true"). \
        #         option("google.cloud.auth.service.account.json.keyfile",
        #                r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\formal-purpose-352005-d58e2dcced94.json"). \
        #         mode("overwrite"). \
        #         csv('gs://etl-d-r/{}/file.csv'.format(table), header=True)