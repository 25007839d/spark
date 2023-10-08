from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, split, explode_outer, lpad, when

spark = SparkSession.builder.appName('oracle_connection') \
    .getOrCreate()

class Filter:

    def state(self,df,state):
        '''
        :param df: pass df
        :param state: state column value
        :return: df
        '''
        df =df.filter(col("state") == "NJ")


        return df

    def table(self,df,table_id):    1`12
        '''
        :param df: pass data frame
        :param table_id: table_id value
        :return: data frame
        '''

        df = df.filter(col("TABLE_NUMBER") == table_id)

        return df

    def id_no(self,df,id_col_name):
        '''

        :param df: pass df
        :param id_col_name:pass column name which you wand
        :return: df
        '''

        df = df.withColumn(id_col_name,monotonically_increasing_id()+1001)
        return df

    def rename(self,df,p_column,n_column):
        '''

        :param df: pass df
        :param p_column: present column list
        :param n_column: next want column list
        :return: df
        '''

        list = n_column   # indexing list
        ind = 0             # for indexing list value
        if p_column!= ['']: # if column is empty
            for i in p_column:

                df = df.withColumnRenamed(i,list[ind])
                ind +=1
            return df

        else:
            return df

    def split(self,df,column,seprator):
        '''

        :param df: pass df
        :param column: pass column for split
        :param seprator: split by seprator
        :return:
        '''

        sep = seprator
        ind = 0
        if column and seprator !=['']:

            for i in column:

                df = df.withColumn(i,split(i,sep[ind]))

                ind +=1
            return df
        else:
            return df

    def explod(self,df,e_column):
        '''

        :param df: pass df
        :param e_column: exploding column
        :return: df
        '''

        if e_column  != ['']:

            for i in e_column:
                df = df.withColumn(i,explode_outer(i))

            return df
        else:
            return df

    def lpad(self,df,l_column,count,value):
        '''

        :param df: pass df
        :param l_column: column for lpad
        :param count: how many count position
        :param value:which value for lapad
        :return: df
        '''

        try:
            value = str(value)
            count = int(count)
            df = df.withColumn(l_column, lpad(l_column,count, value))

            return df
        except:
            print('error')

    def replace(self,df,r_column,c_value,r_value):

        if r_column and c_value and r_value == None:

            df = df.withColumn(r_column,when(col(r_column) == c_value, r_value).otherwise(col(r_column)))
            return df
        else:
            return df