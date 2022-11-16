import tweepy  # install tweepy version 4.10.0
import json
import pandas as pd
import requests
import os
import glob
from time import sleep

# JSON to NDJSON conversion
# https://onexlab-io.medium.com/elasticsearch-json-data-ingestion-b275bc0d1622
# https://github.com/mradamlacey/json-to-es-bulk
# node index.js -f "C:\Users\CAI Yiling\Desktop\GitProjet\ProjetTwitter_IDF\data\tweets_iphone_corri.json" --index twitter --type _doc

# Authentification information
auth_param = {
'access_token': "724143775677845505-WQWYBuUzbucqX8ytKfWz2oZ8XCdekYI",
'access_token_secret': "albiqWEFTUES7K90bPeCItiBFqQdQRzx7eflOQySDwX1O",
'consumer_key': "LrCFkh2aFVDpD0sDrY2HnSXTP",
'consumer_secret' : "uAUvnTDCiIVNV3dLPjDJDkRWglCNQUcd1POFyN29ZGLFN8kh1",
'bearer_token': "AAAAAAAAAAAAAAAAAAAAAGmjhwEAAAAA2bw4J5CXp748WcaLSb5CVwVu5W8%3DCC6hm0yObWyuyTIZu15tTpg2DKVXvhOaUhD9YScLCTouUkxlTA"
}

# Define query parameters: https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query
# Essentiel twitter API user can only search for recent tweets (last seven days) with a limit of 100 tweets per query and a maximum of 500k per month.
# Twitter filed : https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
# Fields with access : 'id', 'text', 'attachments','author_id','context_annotations', 'conversation_id','created_at', 'entities', 'geo',
# 'in_reply_to_user_id', 'lang', 'possibly_sensitive', 'referenced_tweets', 'reply_settings', 'source', 'withheld', 'public_metrics'
# Fields with No access: promoted_metrics, non_public_metrics, organic_metrics
query_param = {
'query':'#iphone14 OR iphone14 OR #iphone14promax OR iphone14promax OR #iphone14pro OR iphone14pro -is:retweet',
'user_fields' : 'username,public_metrics,description,location',
'tweet_fields':'created_at,geo,public_metrics,text,source,lang,reply_settings,possibly_sensitive,entities,context_annotations',
'start_time': "2022-11-11T00:00:00.000Z",
'end_time': "2022-11-12T00:00:00.000Z",
'max_results' : 100
}

query_count_param = {
'query' : "#iphone14 OR iphone14 OR #iphone14promax OR iphone14promax OR #iphone14pro OR iphone14pro -is:retweet",
'start_time': "2022-11-11T23:00:00.000Z",
'end_time': "2022-11-12T00:00:00.000Z",
'granularity' : 'hour',
} 

cmd = os.getcwd()
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
    df.to_csv(file_name)

def combine_csv_file():
    files = glob.glob('*.{}'.format('csv'))
    print(files)

    df_append = pd.DataFrame()
    for file in files:
                df = pd.read_csv(file)
                df_append = df_append.append(df, ignore_index=True)
    df_append.to_csv('twitter-data.csv')

if __name__=='__main__':
    d = GET_TWEETS(auth_param, query_param, query_count_param, nexttoken=None)
    tweets_data = d.recent_tweets()
    nexttoken = tweets_data['meta']['next_token']
    totalcount = 0
    resultcount = tweets_data['meta']['result_count']
    totalcount +=resultcount
    print("Next Token: ", nexttoken)
    print("Result count: ", resultcount)
    save_data_json_file(cmd, str(nexttoken)+'.'+'tweets_iphone.json', tweets_data)
    save_data_csv_file(cmd, str(nexttoken)+'.'+'tweets_iphone.csv', tweets_data)

    while nexttoken is not None:
        sleep(1)
        d = GET_TWEETS(auth_param, query_param, query_count_param, nexttoken)
        tweets_data = d.recent_tweets()
        nexttoken = tweets_data['meta']['next_token']
        resultcount = tweets_data['meta']['result_count']
        totalcount += resultcount
        print("Next Token: ", nexttoken)
        print("Result count: ", resultcount)
        print("Total count: ", totalcount)
        save_data_json_file(cmd, str(nexttoken)+'.'+'tweets_iphone.json', tweets_data)
        save_data_csv_file(cmd, str(nexttoken)+'.'+'tweets_iphone.csv', tweets_data)

