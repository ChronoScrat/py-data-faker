import pyspark
from pyspark.sql import SparkSession
from functools import reduce

class DataGenerator:
    def __init__(self,spark: SparkSession, database: str):
        self.spark = spark
        self.database = database
    
    def generate_and_write_data(self,schema):
        spark = self.spark
        database = self.database
        for table in schema['tables']:

            df = spark.range(table['rows']).toDF("rowID")
            for col in table['columns']:
                df = df.withColumn(col['name'], col['column'])
            df = df.drop("rowID")

            df.write.saveAsTable(name=f"{database}.{table['name']}", mode="overwrite")
        