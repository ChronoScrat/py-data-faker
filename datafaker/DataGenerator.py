# Data Generator

# This file implements the DataGenerator class, which is responsible
# for actually generating the data and storing it into the Hive database.

# Code is adpted from dunnhumby's original implementation in Scala.
# Please check: https://github.com/dunnhumby/data-faker

from pyspark.sql import SparkSession
class DataGenerator:
    """The DataGenerator class creates an instance to generate the fake data.

    It requires an active SparkSession, a database and a Schema object to generate
    the data. As such, before creating a new instance of this class, you should start
    a SparkSession and call `parse_schema_from_file()` on a schema file.

    Args:
        spark (SparkSession): A running Spark Session initiated with `SparkSession.builder`
        database (str): A database name accessible in the SparkSession provided.
    
    Example:
        Initiate an instance of DataGenerator
        >>> data_generator = DataGenerator(spark = spark, database = 'teste_db')
        
        If you have a Schema object, you can then generate the data with
        >>> data_generator.generate_and_write_data(schema = my_schema)
    """
    def __init__(self,spark: SparkSession,database: str):
        self._spark = spark
        self._database = database
    
    def generate_and_write_data(self, schema):
        """Generates the fake data from a provided schema and writes it to a database through the
        Spark Session.

        Args:
            schema (Schema): A Schema object with a list of tables. Provided by `parse_schema_from_file()`.
        """
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
        