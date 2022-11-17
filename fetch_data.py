import tweepy  # install tweepy version 4.10.0
import json
import pandas as pd
import requests
import os
import glob
from time import sleep
from elasticsearch import Elasticsearch, helpers

# Twitter API Authentification information
auth_param = {
'access_token': "724143775677845505-WQWYBuUzbucqX8ytKfWz2oZ8XCdekYI",
'access_token_secret': "albiqWEFTUES7K90bPeCItiBFqQdQRzx7eflOQySDwX1O",
'consumer_key': "LrCFkh2aFVDpD0sDrY2HnSXTP",
'consumer_secret' : "uAUvnTDCiIVNV3dLPjDJDkRWglCNQUcd1POFyN29ZGLFN8kh1",
'bearer_token': "AAAAAAAAAAAAAAAAAAAAAGmjhwEAAAAA2bw4J5CXp748WcaLSb5CVwVu5W8%3DCC6hm0yObWyuyTIZu15tTpg2DKVXvhOaUhD9YScLCTouUkxlTA"
}

# Define twitter query parameters: https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query
"""
Note : Essentiel twitter API user can only search for recent tweets (last seven days) with a limit of 100 tweets per query and a maximum of 500k per month.
Twitter filed : https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
Fields with access : 'id', 'text', 'attachments','author_id','context_annotations', 'conversation_id','created_at', 'entities', 'geo',
'in_reply_to_user_id', 'lang', 'possibly_sensitive', 'referenced_tweets', 'reply_settings', 'source', 'withheld', 'public_metrics'
Fields with No access: promoted_metrics, non_public_metrics, organic_metrics
"""
query_param = {
'query':'#iphone14 OR iphone14 OR #iphone14promax OR iphone14promax OR #iphone14pro OR iphone14pro -is:retweet',
'user_fields' : 'username,public_metrics,description,location',
'tweet_fields':'created_at,geo,public_metrics,text,source,lang,reply_settings,possibly_sensitive,entities,context_annotations',
'start_time': "2022-11-09T17:00:00.000Z",
'end_time': "2022-11-15T07:16:39.000Z",
'max_results' : 100
}

query_count_param = {
'query' : "#iphone14 OR iphone14 OR #iphone14promax OR iphone14promax OR #iphone14pro OR iphone14pro -is:retweet",
'start_time': "2022-11-11T23:00:00.000Z",
'end_time': "2022-11-12T00:00:00.000Z",
'granularity' : 'hour',
} 

# Elasticsearch remote connection parameters
cloudid = 'ptut_iphone:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQzZTRkMDg0Mzg1ZDA0YTY4OWQzZjExMWU1ZmVkY2U3OSRiNzY1OGU0YjQzMDA0ZTE3YTI1M2Y1Y2E0MWMxZTFiMw=='
username = 'elastic'
password = '1ywWvHUn7t47cyZucTqhtZ43'
crt = 'A2525B64D8BFD084D946539261844AC9A3F7DBDC.crt'

# Connect to twitter API2 and get recent tweets. 
class GET_TWEETS:
    def __init__(self, auth_param, query_param, query_count_param, nexttoken):
        self.auth_param = auth_param
        self.query_param = query_param
        self.query_count_param = query_count_param
        self.nexttoken = nexttoken

    # Connection&Authentification : Connect to twitter via tweepy.client : https://docs.tweepy.org/en/stable/client.html
    def twitter_connection_endpoint(self):
        try:
            client = tweepy.Client(**self.auth_param,
                                return_type=requests.Response,
                                wait_on_rate_limit=False)
            return client
        except Exception as e:
            print(e)

    # get recent tweets contents
    def recent_tweets(self):
        client = self.twitter_connection_endpoint()
        tweets = client.search_recent_tweets(**self.query_param, next_token=self.nexttoken)
        if tweets.status_code != 200:
            raise Exception(tweets.status_code, tweets.text)
        return tweets.json()

    # count recent tweets
    def recent_tweets_count(self):
        client = self.twitter_connection_endpoint()
        tweets_count = client.get_recent_tweets_count(**self.query_count_param)
        if tweets_count.status_code != 200:
            raise Exception(tweets_count.status_code, tweets_count.text)
        return tweets_count.json()
    
# save data to json file
def save_data_json_file(cmd, file_name, tweets):
    # Change directory
    chcmd = cmd + '/data/json'
    os.chdir(chcmd)

    # how to save data to json file https://www.w3schools.com/python/python_json.asp
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(tweets, f, indent=2, ensure_ascii=False)

# save data to csv file
def save_data_csv_file(cmd,file_name, tweets):
    # Change directory
    chcmd = cmd + '/data/csv'
    os.chdir(chcmd)
    # Extract "data" from dictionary
    tweets_data = tweets.get('data')
    # Convert json to pandas dataframe
    df = pd.json_normalize(tweets_data)
    # Save data to csv file
    df.to_csv(file_name, index=False)

def combine_csv_file(cmd, filename):
    # Change directory
    chcmd = cmd + '/data/csv'
    os.chdir(chcmd)
    # get all csv files
    files = glob.glob('*.{}'.format('csv'))
    print(files)

    # create a new pandas dataframe
    df_append = pd.DataFrame()

    # read all csv files to the pandas dataframe
    for file in files:
        df = pd.read_csv(file)
        df_append = df_append.append(df, ignore_index=True)
    # write to csv file
    chcmd = cmd + '/data/csv/combined'
    os.chdir(chcmd)
    df_append.to_csv(filename)

# Connect to remote Elasticsearch
def elasticsearch_connection():
    # connect to remote Elasticsearch
    es = Elasticsearch(
        cloud_id=cloudid,
        basic_auth=(username, password),
        ca_certs=crt
    )
    print(es.info())
    return(es)
    
if __name__=='__main__':
    totalcount = 0
    cmd = os.getcwd()
    nexttoken = None
    while True :
        sleep(1)
        tweets_data = GET_TWEETS(auth_param, query_param, query_count_param, nexttoken).recent_tweets()
        try:
            nexttoken = tweets_data['meta']['next_token']
        except Exception:
            break
        resultcount = tweets_data['meta']['result_count']
        totalcount += resultcount
        print("Next Token: ", nexttoken)
        print("Result count: ", resultcount)
        print("Total count: ", totalcount)
        save_data_csv_file(cmd, str(nexttoken)+'.'+'tweets_iphone.csv', tweets_data)

    combine_csv_file(cmd, 'twitter-data.csv')
    es = elasticsearch_connection()
    chcmd = cmd + '/data/csv/combined'
    os.chdir(chcmd)
    df = pd.read_csv("twitter-data.csv")
    json_str = df.to_json(orient='records')
    json_records = json.loads(json_str)
    helpers.bulk(es,json_records, index='test')
    



