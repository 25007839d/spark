
from pyspark.sql import *

spark = SparkSession.builder.appName('oracle_connection')\
        .config('spark.jars',r"C:\Big Data\ojdbc10.jar")\
        .getOrCreate()

driver = 'oracle.jdbc.driver.OracleDriver'
url = 'jdbc:oracle:thin:@localhost:1521/xe'
user='sys'
password='sys'
table1 = 'hr.employees'
table2 = 'countries'
table3 = 'departments'
table4 = 'jobs'
table5 = 'job_history'
table6 = 'locations'
table7 = 'regions'

#getting whole table data

class hr:

    def emp(self):
        employees = spark.read.format('jdbc')\
                    .option('driver',driver)\
                    .option('url',url)\
                    .option('user',user)\
                    .option('dbtable',table1)\
                    .option('password',password).load()


        return employees

    def departments(self):
        departments = spark.read.format('jdbc')\
                    .option('driver',driver)\
                    .option('url',url)\
                    .option('user',user)\
                    .option('dbtable',table3)\
                    .option('password',password).load()

        return departments

    def countries(self):
        countries = spark.read.format('jdbc')\
                    .option('driver',driver)\
                    .option('url',url)\
                    .option('user',user)\
                    .option('dbtable',table2)\
                    .option('password',password).load()
        return countries

    def jobs(self):
        jobs = spark.read.format('jdbc')\
                    .option('driver',driver)\
                    .option('url',url)\
                    .option('user',user)\
                    .option('dbtable',table4)\
                    .option('password',password).load()
        return jobs

    def job_history(self):
        job_history = spark.read.format('jdbc')\
                    .option('driver',driver)\
                    .option('url',url)\
                    .option('user',user)\
                    .option('dbtable',table5)\
                    .option('password',password).load()
        return job_history

    def locations(self):
        locations = spark.read.format('jdbc')\
                    .option('driver',driver)\
                    .option('url',url)\
                    .option('user',user)\
                    .option('dbtable',table6)\
                    .option('password',password).load()
        return locations
    def regions(self):
        regions = spark.read.format('jdbc')\
                    .option('driver',driver)\
                    .option('url',url)\
                    .option('user',user)\
                    .option('dbtable',table7)\
                    .option('password',password).load()
        return regions




hr = hr()
employees = hr.emp()
employees.show()
# departments = hr.departments()
# countries = hr.countries()
# jobs = hr.jobs()
# job_history = hr.job_history()
# locations = hr.locations()
# regions = hr.regions()

# print("+-----------------schema----------------+")
# employees.printSchema()
# print("+------------------Table----------------+")
# employees.show()
# print("+-----------------schema----------------+")
# departments.printSchema()
# print("+------------------Table----------------+")
# departments.show()
# print("+-----------------schema----------------+")
# countries.printSchema()
# print("+------------------Table----------------+")
# countries.show()
# print("+-----------------schema----------------+")
# jobs.printSchema()
# print("+------------------Table----------------+")
# jobs.show()
# print("+-----------------schema----------------+")
# job_history.printSchema()
# print("+------------------Table----------------+")
# job_history.show()
# print("+-----------------schema----------------+")
# locations.printSchema()
# print("+------------------Table----------------+")
# locations.show()
# print("+-----------------schema----------------+")
# regions.printSchema()
# print("+------------------Table----------------+")
# regions.show()