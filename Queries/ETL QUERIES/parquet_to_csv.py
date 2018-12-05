import sys
import os
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Parquet to CSV').getOrCreate()
#assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext
sc.setLogLevel('WARN')


def main(inputs,output):
    
    #For reading Parquet input files partitioned on Product Categories
    input_df = spark.read.parquet(inputs)
    input_df = input_df.repartition(96) 
    input_df.coalesce(1).write.partitionBy('product_category').option("header","true").csv(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)

