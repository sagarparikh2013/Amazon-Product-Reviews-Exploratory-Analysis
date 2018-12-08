# Amazon-Product-Reviews-Exploratory-Analysis
Exploratory Analysis of Amazon Product Reviews Dataset comprising of various categories spanning over 20 years
Visualizations have been done on this dataset and made public here:
[Amazon Product Reviews Analysis](https://public.tableau.com/profile/sagar.parikh#!/vizhome/AmazonProductReviewsInsights/InsightsStory)
Dataset Overview:
Amazon sells millions of products every year and most products have multiple user reviews and ratings. 
This dataset can potentially be used to find some interesting correlations between features like Price, Ratings, Reviews etc.

The Amazon Review Dataset is available on the AWS PUBLIC DATASETS which contains reviews and ratings from the year 1996 to 2015. 
Each review in the dataset has information about like reviewerId, productId, reviewSummary, reviewText, ratings,reviewTime and so on.

The metadata as found on the UCSD website contains information about the product such as productId, title, price, sales-rank,
brand, categories etc. The metadata is about 3 GB in size.

Data Analysis:
As the size of the dataset is huge, ETL (Extraction, Transformation, and Load) process will be used to focus only on reviews in the last few years as well remove duplicate reviews on near identical products. This will also filter out the products which have less than 10 reviews per product.

Using Big Data tools, the aim is to uncover correlations between various features like Price Vs Ratings, Price Vs Review Sentiment, Sales Rank Vs Reviews, Sales Rank Vs Price etc. Tableau is used for graphing these co-relations.

Intention is also to answer some intriguing questions like below (but not limited to these):

	- What are the 3 most reviewed categories? What are the most reviewed products in them?
	- On which day of week are people most likely to write product reviews? Is it the weekend?
	- Who are the most influential reviewers based on based on total reviews and helpfulness
	of their reviews?
	- Which category has the most helpful reviews?
	- What adjectives/adverbs are used in the reviews which are considered most helpful by
	other users?
	- What is the average length of reviews which are considered most useful by others?
	- Product counts in each category
	- Month wise number of reviews to uncover any seasonal trends
	- Total number of reviewers and unique products in the dataset

Technology/Tools:

	- Spark
	- Tableau, Matplotlib & D3js
	- NLTK / equivalent library for parts of speech tagging and sentiment analysis of reviews
	