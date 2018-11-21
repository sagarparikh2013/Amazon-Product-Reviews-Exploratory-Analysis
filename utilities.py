from pyspark import SparkConf, SparkContext
import sys
import random
import operator

COMPLETE_ETLDATA_PATH = '/user/parikh/amazon_etl_final'

# /user/parikh/amazon_datasets/metadata.json.gz
META_DATA_PATH = '/user/parikh/amazon_datasets'


def get_metadata_dataframe(spark_context):
	metadata = spark_context.read.json(META_DATA_PATH, schema=comments_schema)
	return metadata


def get_complete_dataframe(spark_context):
	complete_df = spark_context.read.parquet(COMPLETE_ETLDATA_PATH, schema=comments_schema)
	return complete_df
