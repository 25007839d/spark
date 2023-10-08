from pyspark.sql.types import StructField, StringType, DateType, TimestampType, DecimalType,StructType


class Schema:
    def tbl_schema(self):

        schema = StructType([StructField('country',StringType()),
                             StructField('state',StringType()),
                             StructField('table_number',StringType()),
                             StructField('common_state',StringType()),
                             StructField('EFFECTIVE_DATE',TimestampType()),
                             StructField('EXPIRATION_DATE',TimestampType()),
                             StructField('class_code',StringType()),
                             StructField('coverage',StringType()),
                             StructField('symbol',StringType()),
                             StructField('construction_code',StringType()),
                             StructField('factor',DecimalType(38, 3))

        ])
