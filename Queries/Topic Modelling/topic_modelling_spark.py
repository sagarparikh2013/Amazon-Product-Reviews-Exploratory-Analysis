import sys
import os
import time

from datetime import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, from_unixtime,broadcast,udf,year
from pyspark.mllib.util import MLUtils

# stuff we'll need for text processing
from nltk.corpus import stopwords
import re as re
from pyspark.ml.feature import CountVectorizer , IDF
# stuff we'll need for building the model

from pyspark.mllib.linalg import Vector, Vectors as MLlibVectors
from pyspark.mllib.clustering import LDA as MLlibLDA, LDAModel
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import CountVectorizerModel, Tokenizer, RegexTokenizer, StopWordsRemover
from pyspark.ml import Pipeline
from nltk.corpus import stopwords

spark = SparkSession.builder.appName('Topic Modelling LDA').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')



def main(inputs):
    
    amazon_schema = types.StructType([
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

    input_df = spark.read.parquet(inputs)
    input_df = input_df.repartition(96) 
    #input_df.show()
    #print("No of rows in input dataset:",inputs," is:",input_df.count())
    StopWords = stopwords.words("english")
    start_time = time.time()

    tokens = input_df.rdd.map(lambda x: x['review_headline'])\
    .filter(lambda x: x is not None)\
    .map( lambda document: document.strip().lower())\
    .map( lambda document: re.split(" ", document))\
    .map( lambda word: [x for x in word if x.isalpha()])\
    .map( lambda word: [x for x in word if len(x) > 3] )\
    .map( lambda word: [x for x in word if x not in StopWords])\
    .zipWithIndex()

    df_txts = spark.createDataFrame(tokens, ["list_of_words",'index'])

    # TF
    cv = CountVectorizer(inputCol="list_of_words", outputCol="raw_features", vocabSize=5000, minDF=10.0)
    cvmodel = cv.fit(df_txts)
    result_cv = cvmodel.transform(df_txts)
    
    # IDF
    idf = IDF(inputCol="raw_features", outputCol="features")
    idfModel = idf.fit(result_cv)
    result_tfidf = idfModel.transform(result_cv) 
    

    #result_tfidf.show()

    num_topics = 10
    max_iterations = 100
    lda = LDA(k=num_topics, maxIter=max_iterations)
    lda_model = lda.fit(result_tfidf.select('index','features'))

    wordNumbers = 5  
    #topicIndices = sc.parallelize(lda_model.describeTopics(maxTermsPerTopic = wordNumbers))
    
    topics = lda_model.describeTopics(maxTermsPerTopic = wordNumbers)    
    topics.show(truncate=False)

if __name__ == '__main__':
    inputs = sys.argv[1]
    # output = sys.argv[2]
    # start_year = sys.argv[3]
    # end_year = sys.argv[4]
    main(inputs)

