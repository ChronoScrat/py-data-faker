#!/usr/bin/python3

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
parser.add_argument("--download", help="Download final database", required=False, action="store_true")



def main():

    # Get args
    args = parser.parse_args()
    
    # Spark Connection
    spark = SparkSession.builder.appName("py-datafaker").enableHiveSupport().getOrCreate()

    # Create database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.database}")
    schema = YamlParser.parse_schema_from_file(args.file)
    dataGenerator = DataGenerator(spark,args.database)

    dataGenerator.generate_and_write_data(schema)

    if (args.download):
        import os
        import pandas
        import subprocess

        os.makedirs(f"/tmp/{args.database}", exist_ok=True)

        for table in schema.tables:
            spark.table(f"{args.database}.{table.name}").toPandas().to_csv(f"/tmp/{args.database}/{table.name}.csv", index=False)
        
        subprocess.run(["tar", "cvzf", f"{args.database}.tar.gz", f"/tmp/{args.database}/"])

if __name__ == "__main__":
    main()
