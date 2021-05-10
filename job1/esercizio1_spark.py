#!/usr/bin/env python3

"""spark application"""
import argparse
from pyspark.sql import SparkSession

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path


# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Spark Wordcount") \
    .getOrCreate()


# read the input file and obtain an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath).cache()
