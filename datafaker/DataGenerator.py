from pyspark.sql import SparkSession
class DataGenerator:
    def __init__(self,spark: SparkSession,database: str):
        self._spark = spark
        self._database = database
    
    def generate_and_write_data(self, schema):
        spark = self._spark
        database = self._database

        for table in schema.tables:

            df = spark.range(table.rows).toDF("rowID")
            for col in table.columns:
                print(col.name)
                df = df.withColumn(col.name, col.column())
            df = df.drop("rowID")

            df.write.saveAsTable(name=f"{database}.{table.name}", mode="overwrite")
        