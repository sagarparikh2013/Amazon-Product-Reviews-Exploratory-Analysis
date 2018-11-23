import sys
from pyspark.sql import SparkSession, functions
import utilities

assert sys.version_info >= (3, 5)   # make sure we have Python 3.5+

def keyWithMaxValue(dict):
    keys = list(dict.keys())
    values = list(dict.values())
    return keys[values.index(max(values))]

def main(inputs):
    reviews_df = utilities.get_completereviews_dataframe(spark)
    #reviews_df = spark.read.csv(sep='\t', path=inputs, schema=utilities.REVIEWS_SCHEMA)

    #find average
    reviews_df = reviews_df.filter(reviews_df.product_id.isNotNull()).select(reviews_df.star_rating).cache()
    mean = reviews_df.agg(functions.avg(reviews_df.star_rating).alias('mean'))
    mean.repartition(1).write.mode('overwrite').csv('average')

    #find median
    median = reviews_df.approxQuantile('star_rating', [0.5], 0.25)
    sc.parallelize(median).repartition(1).saveAsTextFile('median.csv')
    print("The median of the dataset is : " + str(median))

    #find mode
    mode_dict = reviews_df.groupBy('star_rating').agg(functions.count('star_rating').alias('count')).rdd.collectAsMap()
    mode = keyWithMaxValue(mode_dict)
    mode = [mode]
    sc.parallelize(list(mode)).repartition(1).saveAsTextFile('mode.csv')
    print("The mode of the dataset: " + str(mode))

if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata\\sample_us.tsv"
    spark = SparkSession.builder.appName('Spark Cassandra load logs').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+

    main(inputs)