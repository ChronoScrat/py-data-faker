#!/usr/bin/python3

# py-data-faker utility

# This file is simply a utility to make use of the `py-data-faker`
# package. Besides being a command line utility, it provides an
# example on how to use the package on your own implementation.

from pyspark.sql import SparkSession
import logging
import argparse

from datafaker import SchemaParser, DataGenerator

# Argument Paser:
## We need a hive database name and a path to the schema file
parser = argparse.ArgumentParser(
        prog="pyDataFaker",
        description="""Generate fake datasets in Apache Spark and store them in Apache Hive! 
        This utility makes use of py-datafaker to generate fake data from a provided schema and store it
        in Apache Hive or other storage backends."""
    )
parser.add_argument("--database", help="The Hive database name in which the tables will be created. If it does not exist, it will be created.", required=True,action="store")
parser.add_argument("--file", help="The path to the YAML schema file detailing the tables and columns", required=True,action="store")
parser.add_argument("--download", help="[OPTIONAL] If provided, the generated datasets will be downloaded as CSVs into the current directory (archived and compressed)", required=False, action="store_true")



def main():

    # Get args
    args = parser.parse_args()
    
    # Spark Connection
    spark = SparkSession.builder.appName("py-datafaker").enableHiveSupport().getOrCreate()

    # Create database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.database}")
    schema = SchemaParser.parse_schema_from_file(args.file)
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
