import pandas as pd
from sklearn.datasets import fetch_20newsgroups
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from nltk import WordNetLemmatizer
import pyLDAvis
import pyLDAvis.sklearn
import numpy as np
import json
import sys
import ntpath
import os

# to run this file, have a bricks folder in your path with etl_final_2k_2k5 subfolder containing reviews data in parquet format
path = 'bricks\\etl_final_2k_2k5'

categories = ['Electronics', 'Camera', 'Books', 'Kitchen']
# input = sys.argv[1]


class LemmaCountVectorizer(CountVectorizer):
    def build_analyzer(self):
        lemm = WordNetLemmatizer()
        analyzer = super(LemmaCountVectorizer, self).build_analyzer()
        return lambda doc: (lemm.lemmatize(w) for w in analyzer(doc) if (not doc.isdigit()) and len(doc) >= 3)

def yield_reviews(df_pandas):
    print("inside yield reviews")
    for index, rows in df_pandas.iterrows():
        yield rows['review_body']

def get_data_pandas(path, category):
    print("inside get_Data_pandas")
    df = pd.read_parquet(path)
    if category != 'all':
        df = df[df['product_category'] == category]
    df1 = df[['review_body']]
    print(df1.head())
    return yield_reviews(df1)

def preprocess(data):
    print("inside preprocess")
    tf_vectorizer = LemmaCountVectorizer(max_df=0.95, min_df=2, stop_words='english', lowercase=True,
                                         strip_accents='unicode', token_pattern=r'\b[a-zA-Z]{5,}\b')
    vecorized_data = tf_vectorizer.fit_transform(data)
    tf_feature_names = tf_vectorizer.get_feature_names()
    return vecorized_data, tf_feature_names, tf_vectorizer

def train(vectorized_data):
    print("inside train")
    no_features = 1000
    no_topics = 10
    lda = LatentDirichletAllocation(n_components=no_topics, max_iter=100, learning_method='online', learning_offset=50.,
                                    random_state=0).fit(vectorized_data)
    return lda

def display_topics(model, feature_names, no_top_words, file_save):
    print("inside display topics")
    topic_words = {}
    for topic, comp in enumerate(model.components_):
        word_idx = np.argsort(comp)[::-1][:no_top_words]
        # store the words most relevant to the topic
        topic_words[topic] = [feature_names[i] for i in word_idx]
    os.makedirs(os.path.dirname(file_save), exist_ok=True)
    json.dump(topic_words, open(file_save, 'w'))

def sklearn_visualize(trained_model, vectorized_data, vectorizer_object, save_path):
    print("inside sklearn_visualize")
    ## visualize
    prepared_data = pyLDAvis.sklearn.prepare(trained_model, vectorized_data, vectorizer_object, mds='tsne')
    pyLDAvis.save_html(prepared_data, save_path)

def pipeline_execute(input_path, output_file, category):
    print("inside pipeline execute")
    generator_data = get_data_pandas(input_path, category)
    vectorized_data, tf_feature_names, tf_vectorizer = preprocess(generator_data)

    trained_model = train(vectorized_data)
    no_top_words = 10
    display_topics(trained_model, tf_feature_names, no_top_words, ntpath.join(output_file, category, 'lda_results' + category + ".txt"))

    ##visualize
    sklearn_visualize(trained_model, vectorized_data, tf_vectorizer, ntpath.join(output_file, category, 'demo_' + category + ".html"))

for category in categories:
    pipeline_execute(path, ntpath.join(os.getcwd(), 'results'), category)