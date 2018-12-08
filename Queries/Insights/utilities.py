from pyspark import SparkConf, SparkContext
import sys
import random
import operator
from pyspark.sql import SparkSession, functions, types
import json

COMPLETE_PARQUET_DATAPATH = '/user/parikh/etl_final_1995_2015'

# /user/parikh/amazon_datasets/metadata.json.gz
META_DATA_PATH = '/user/parikh/amazon_datasets'

REVIEWS_SCHEMA = types.StructType([
    types.StructField('marketplace',types.StringType()),
    types.StructField('customer_id',types.IntegerType()),
    types.StructField('review_id',types.StringType()),
    types.StructField('product_id',types.StringType()),
    types.StructField('product_parent',types.LongType()),
    types.StructField('product_title',types.StringType()),
    types.StructField('product_category',types.StringType()),
    types.StructField('star_rating',types.IntegerType()),
    types.StructField('helpful_votes',types.IntegerType()),
    types.StructField('total_votes',types.IntegerType()),
    types.StructField('vine',types.StringType()),
    types.StructField('verified_purchase',types.StringType()),
    types.StructField('review_headline',types.StringType()),
    types.StructField('review_body',types.StringType()),
    types.StructField('review_date',types.DateType())])

# METADATA_SCHEMA = types.StructType([
#     types.StructField('asin',types.StringType()),
#     types.StructField('categories',types.ArrayType(elementType=types.StringType())),
#     types.StructField('brand',types.StringType()),
#     types.StructField('description',types.StringType()),
#     types.StructField('imUrl',types.StringType()),
#     types.StructField('price',types.DoubleType()),
#     types.StructField('related',types.IntegerType()),
#     types.StructField('salesRank',types.IntegerType()),
#     types.StructField('related',types.StructType()),
#     types.StructField('title',types.IntegerType())])


# expensive operation: use only when needed
def get_metadata_dataframe(spark):
    metadata = spark.read.json(META_DATA_PATH)
    metadata = metadata.repartition(80)
    return metadata


def get_completereviews_dataframe(spark):
    complete_df = spark.read.parquet(COMPLETE_PARQUET_DATAPATH)
    complete_df = complete_df.repartition(80)
    return complete_df
