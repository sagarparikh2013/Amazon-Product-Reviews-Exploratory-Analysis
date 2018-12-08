import sys
import os
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Parquet to CSV').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')


def main(inputs,output):
    
    #For reading Parquet input files partitioned on Product Categories
    input_df = spark.read.parquet(inputs)
    input_df = input_df.repartition(96) 
    #Saving in tsv format to load datasets in Tableau and visualize results
    input_df.write.partitionBy('product_category').option("header","true").csv(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)

