import tweepy  # install tweepy version 4.10.0
import json
import pandas as pd
import requests
import os
import ndjson

############################  ############################
# Authentification information
access_token = "724143775677845505-WQWYBuUzbucqX8ytKfWz2oZ8XCdekYI"
access_token_secret = "albiqWEFTUES7K90bPeCItiBFqQdQRzx7eflOQySDwX1O"
consumer_key = "LrCFkh2aFVDpD0sDrY2HnSXTP"
consumer_secret = "uAUvnTDCiIVNV3dLPjDJDkRWglCNQUcd1POFyN29ZGLFN8kh1"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAGmjhwEAAAAA2bw4J5CXp748WcaLSb5CVwVu5W8%3DCC6hm0yObWyuyTIZu15tTpg2DKVXvhOaUhD9YScLCTouUkxlTA"

# Define query : https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query
query = '#iphone14 #iphone14pro #iphone14promax'

# Define query start time and end time
starttime = "2022-11-10T00:00:00.000Z"
endtime = "2022-11-11T07:00:00.000Z"
granularity = 'hour'

############################ Connection&Authentification ############################
# Connect to twitter via tweepy.client : https://docs.tweepy.org/en/stable/client.html
client = tweepy.Client(bearer_token=bearer_token,
                       consumer_key=consumer_key,
                       consumer_secret=consumer_secret,
                       access_token=access_token,
                       access_token_secret=access_token_secret,
                       return_type=requests.Response,
                       wait_on_rate_limit=True)
############################ Search contents############################
# Essentiel twitter API user can only search for recent tweets (last seven days) with a limit of 100 tweets per query and a maximum of 500k per month.
# Twitter filed : https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
# Fields with access : 'id', 'text', 'attachments','author_id','context_annotations', 'conversation_id','created_at', 'entities', 'geo',
# 'in_reply_to_user_id', 'lang', 'possibly_sensitive', 'referenced_tweets', 'reply_settings', 'source', 'withheld', 'public_metrics'
# Fields with No access: promoted_metrics, non_public_metrics, organic_metrics
tweets = client.search_recent_tweets(
    query=query,
    end_time=endtime,
    start_time=starttime,
    tweet_fields=['id', 'text', 'attachments','author_id','context_annotations', 'conversation_id','created_at', 'entities', 'geo',
    'in_reply_to_user_id', 'lang', 'possibly_sensitive', 'referenced_tweets', 'reply_settings', 'source', 'withheld', 'public_metrics'],
    max_results=10)

# count recent tweets
tweets_count = client.get_recent_tweets_count(query=query,
                                              end_time=endtime,
                                              granularity=granularity,
                                              start_time=starttime)

###########################Save data###########################
# Save data as dictionary
tweets_dict = tweets.json()
tweets_count_dict = tweets_count.json()

# Change directory
cmd = os.getcwd()
chcmd = cmd + '/data'
os.chdir(chcmd)


# Save data to json file https://www.w3schools.com/python/python_json.asp
with open('tweets_iphone.json', 'w', encoding='utf-8') as f:
    json.dump(tweets_dict, f, indent=2, ensure_ascii=False)

with open('tweets_count_iphone.json', 'w', encoding='utf-8') as f:
    json.dump(tweets_count_dict, f, indent=2, ensure_ascii=False)

#Save data to local csv file#
# Extract "data" from dictionary
tweets_data = tweets_dict.get('data')
# tweets_data = tweets_dict['data']
tweets_count_data = tweets_count_dict['data']

# Convert json to pandas dataframe
df = pd.json_normalize(tweets_data)
df_count = pd.json_normalize(tweets_count_data)

# Save data to csv file
df.to_csv("tweets_iphone.csv")
df_count.to_csv("tweets_count_iphone.csv")

# JSON to NDJSO conversion
# https://onexlab-io.medium.com/elasticsearch-json-data-ingestion-b275bc0d1622
# https://github.com/mradamlacey/json-to-es-bulk
# node index.js -f "C:\Users\CAI Yiling\Desktop\GitProjet\ProjetTwitter_IDF\data\tweets_iphone_corri.json" --index twitter --type _doc

