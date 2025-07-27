import pyspark
from pyspark.sql import SparkSession
import logging
import argparse

from datafaker import YamlParser, DataGenerator

# Argument Paser:
## We need a hive database name and a path to the schema file
parser = argparse.ArgumentParser(
        prog="pyDataFaker",
        description="Fake data!"
    )
parser.add_argument("--database", help="Hive database name", required=True,action="store")
parser.add_argument("--file", help="Path to YAML schema file", required=True,action="store")



def main():

    # Get args
    args = parser.parse_args()
    
    # Spark Connection
    spark = SparkSession.builder.appName("py-datafaker").enableHiveSupport().getOrCreate()

    # Create database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.database}")
    schema = YamlParser.parse_schema_from_file(args.file)
    print(type(spark))
    dataGenerator = DataGenerator(spark,args.database)

    dataGenerator.generate_and_write_data(schema)


if __name__ == "__main__":
    main()
