import gzip
import shutil
import os
import wget
import csv
import linecache
from shutil import copyfile
import numpy as np
import pandas as pd
import json
import tweepy
from tweepy import OAuthHandler

# dataset_URL = "" #@param {type:"string"}


#Downloads the dataset (compressed in a GZ format)
#!wget dataset_URL -O clean-dataset.tsv.gz
# wget.download(dataset_URL, out='clean-dataset.tsv.gz')


# #Unzips the dataset and gets the TSV dataset
# with gzip.open('clean-dataset.tsv.gz', 'rb') as f_in:
#     with open('clean-dataset.tsv', 'wb') as f_out:
#         shutil.copyfileobj(f_in, f_out)

# #Deletes the compressed GZ file
# os.unlink("clean-dataset.tsv.gz")

#Gets all possible languages from the dataset
# df = pd.read_csv('clean-dataset.tsv',sep=",")
# # Filter english lanaguage
# print(df.columns)
# print(df)

df = pd.read_csv('tweets_emotion_sg.csv',sep=",")
# Filter english lanaguage
print(df.columns)
print(df)



# Authenticate
CONSUMER_KEY = 'YOUR_CONSUMER_KEY'
CONSUMER_SECRET_KEY = 'YOUR_CONSUMER_SECRET_KEY'
ACCESS_TOKEN_KEY = 'YOUR_ACCESS_TOKEN_KEY'
ACCESS_TOKEN_SECRET_KEY = 'YOUR_ACCESS_TOKEN_SECRET_KEY'

#Creates a JSON Files with the API credentials
with open('api_keys.json', 'w') as outfile:
    json.dump({
    "consumer_key":CONSUMER_KEY,
    "consumer_secret":CONSUMER_SECRET_KEY,
    "access_token":ACCESS_TOKEN_KEY,
    "access_token_secret": ACCESS_TOKEN_SECRET_KEY
     }, outfile)

#The lines below are just to test if the twitter credentials are correct
# Authenticate
auth = tweepy.AppAuthHandler(CONSUMER_KEY, CONSUMER_SECRET_KEY)

api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

if (not api):
   print ("Can't Authenticate")
   sys.exit(-1)
else :
    print("Authenticated")



