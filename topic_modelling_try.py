import sys
import os
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, from_unixtime,broadcast,udf,year
from pyspark.mllib.util import MLUtils
from pyspark.mllib.clustering import LDA
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel, Tokenizer, RegexTokenizer, StopWordsRemover
from pyspark.ml import Pipeline
spark = SparkSession.builder.appName('Read Parquets S3 Categories').getOrCreate()
#assert spark.version >= '2.3' # make sure we have Spark 2.3+
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

    tokenizer = Tokenizer(inputCol="review_headline", outputCol="words")
    wordsDataFrame = tokenizer.transform(input_df)
    wordsDataFrame.select('product_id','product_category','star_rating','review_headline','words').show()

    cv_tmp = CountVectorizer(inputCol="words", outputCol="tmp_vectors")
    cv_tmp_model = cv_tmp.fit(wordsDataFrame)
    #cv_tmp_model.show()

    top20 = list(cv_tmp_model.vocabulary[0:20])
    more_than_3_charachters = [word for word in cv_tmp_model.vocabulary if len(word) <= 3]
    contains_digits = [word for word in cv_tmp_model.vocabulary if any(char.isdigit() for char in word)]

    stopwords = []  #Add additional stopwords in this list

    #Combine the three stopwords
    stopwords = stopwords + top20 + more_than_3_charachters + contains_digits

    #Remove stopwords from the tokenized list
    remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords = stopwords)
    wordsDataFrame = remover.transform(wordsDataFrame)

    #Create a new CountVectorizer model without the stopwords
    cv = CountVectorizer(inputCol="filtered", outputCol="vectors")
    cvmodel = cv.fit(wordsDataFrame)
    df_vect = cvmodel.transform(wordsDataFrame)

    df_vect.select('product_category','review_headline','words','filtered','vectors').show()

    #transform the dataframe to a format that can be used as input for LDA.train. LDA train expects a RDD with lists,
    #where the list consists of a uid and (sparse) Vector
    def parseVectors(line):
        return [line[2], line[0]]


    sparsevector = df_vect.select('vectors', 'review_headline', 'product_id').rdd.map(parseVectors)

    #Train the LDA model
    model = LDA.train(sparsevector, k=5, seed=1)

    #Print the topics in the model
    topics = model.describeTopics(maxTermsPerTopic = 10)
    for x, topic in enumerate(topics):
        print ('topic nr: ' + str(x))
        words = topic[0]
        weights = topic[1]
        for n in range(len(words)):
            print (cvmodel.vocabulary[words[n]] + ' ' + str(weights[n]))

if __name__ == '__main__':
    inputs = sys.argv[1]
    # output = sys.argv[2]
    # start_year = sys.argv[3]
    # end_year = sys.argv[4]
    main(inputs)

