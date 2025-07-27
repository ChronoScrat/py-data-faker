# Data Generator

# This file implements the DataGenerator class, which is responsible
# for actually generating the data and storing it into the Hive database.

# Code is adpted from dunnhumby's original implementation in Scala.
# Please check: https://github.com/dunnhumby/data-faker

from pyspark.sql import SparkSession
class DataGenerator:
    def __init__(self,spark: SparkSession,database: str):
        self._spark = spark
        self._database = database
    
    def generate_and_write_data(self, schema):
        spark = self._spark
        database = self._database

        for table in schema.tables:

            partitions = table.partitions
            if not partitions:
                partitions = []

            df = spark.range(table.rows).toDF("rowID")
            for col in table.columns:
                df = df.withColumn(col.name, col.column())
            df = df.drop("rowID")

            df.write.saveAsTable(name=f"{database}.{table.name}", mode="overwrite", partitionBy = partitions)
        