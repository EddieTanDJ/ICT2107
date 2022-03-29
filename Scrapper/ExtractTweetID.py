""" 
This script extracts the tweet ID from the tweet text and spilt into multiple file for Tweets hydration
"""
import pandas as pd
# Change to the CSV file you want to extract IDs from
df = pd.read_csv('tweets_india.csv')
# Extract first column
first_column = df.iloc[:, 0]
#Save into CSV file using pandas
# first_column.to_csv('tweet_id_cleaned.csv', header=False, index=False)
# no of csv files with row size
# K is the number of files you want to split the data into
# size is the number of row in each file
k = 10
size = 4000000
for i in range(k):
    df = first_column[size*i:size*(i+1)]
    # Change it to the file name you want to save it to
    df.to_csv(f'tweets_usa_id_cleaned_{i+1}.csv', header=False, index=False)
 