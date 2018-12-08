import sys
import os
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Parquet to CSV Single Category').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')


def main(inputs,output,product_category):

    #For reading Parquet input files partitioned on Product Categories
    input_df = spark.read.parquet(inputs)
    input_df = input_df.repartition(96)
    final_df = input_df.withColumn("product_category",functions.lit(product_category))
    final_df.select('marketplace','customer_id','review_id','product_id','product_parent','product_title','product_category',\
        'star_rating','helpful_votes','total_votes','vine','verified_purchase','review_headline','review_body','review_date')\
    .write.option('sep','\t').option("header","true").csv(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    product_category = sys.argv[3]

    main(inputs,output,product_category)

