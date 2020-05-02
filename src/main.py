#!/usr/bin/python
import argparse
import configparser
import importlib
import time
import os
import sys

from pyspark.sql import SparkSession

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

config = configparser.ConfigParser()
config.read('../config/dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

__author__ = 'dre'


def create_spark_session():
    """
    Creates the spark session

    :return: None
    """

    # We configure spark to download the necessary hadoop-aws dependencies
    # and se the fileoutputcommitter to 2 for better handling of writing data to s3
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.0",
                ) \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()
    return spark


def main():
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="The name of the job module you want "
                                                                                "to run. (ex: poc will run job on "
                                                                                "jobs.poc package)")
    parser.add_argument('--input-data', type=str, required=True, dest='input_name',
                        help="The path to the directory that contains the input data")
    parser.add_argument('--output-data', type=str, required=True, dest='output_name',
                        help="The path to the directory that contains the output data")

    args = parser.parse_args()
    print("Called with arguments: %s" % args)

    print('\nRunning job %s...\ninput  is %s\noutput is %s\n' % (args.job_name, args.input_name, args.output_name))

    job_module = importlib.import_module('jobs.%s' % args.job_name)

    start = time.time()
    spark = create_spark_session()

    # Call the job provided in the arguments
    job_module.analyze(spark, args.input_name, args.output_name)
    end = time.time()

    print("\nExecution of job %s took %s seconds" % (args.job_name, end - start))


if __name__ == "__main__":
    main()
