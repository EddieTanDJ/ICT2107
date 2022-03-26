from turtle import title
from psaw import PushshiftAPI    #library Pushshift
import datetime as dt            #library for date management
import pandas as pd                      #library for data manipulation
import html



api = PushshiftAPI()

""" 
Data Crawler for subreddit posts
"""
def data_prep_posts(title, subreddit, start_time, end_time, filters, limit):
    if(len(filters) == 0):
        filters = ['id', 'author', 'created_utc',
                   'domain', 'url',
                   'title', 'num_comments']                 
                   #We set by default some useful columns

    posts = list(api.search_submissions(
        title = title, #Title of the post
        subreddit=subreddit,   #Subreddit we want to audit
        after=start_time,      #Start date
        before=end_time,       #End date
        filter=filters,        #Column names we want to retrieve
        limit=limit))          ##Max number of posts

    return pd.DataFrame(posts) #Return dataframe for analysis

""" 
Data Crawler for comments in subreddit
"""
"""FOR COMMENTS"""
def data_prep_comments(term, subreddit, start_time, end_time, filters, limit):
    if (len(filters) == 0):
        filters = ['id', 'author', 'created_utc',
                   'body', 'permalink']
                   #We set by default some usefull columns 

    comments = list(api.search_comments(
        q=term,                 #Title we want to audit
        subreddit = subreddit,   #Subreddit we want to audit
        after=start_time,       #Start date
        before=end_time,        #End date
        filter=filters,         #Column names we want to retrieve
        limit=limit))           #Max number of comments
    return pd.DataFrame(comments) #Return dataframe for analysis

def main():
    subreddit = "Singapore"     #Subreddit we are auditing
    title = "lockdown"   #Title of the post we are auditing
    start_time = int(dt.datetime(2020, 3, 24).timestamp())  
                                     #Starting date for our search
    end_time = int(dt.datetime(2020,4,30).timestamp())   
                                     #Ending date for our search
    filters = []                     #We don´t want specific filters
    limit = 1000000                    #Elelemts we want to recieve

    """Here we are going to get subreddits for a brief analysis"""
    #Call function for dataframe creation of comments
    df = data_prep_comments(title, subreddit,start_time,
                         end_time,filters,limit) 
 
    # Drop the column on timestamp format
    df['datetime'] = df['created_utc'].map(
        lambda t: dt.datetime.fromtimestamp(t))
    df = df.drop('created_utc', axis=1) 
    # Sort the Row by datetime               
    df = df.sort_values(by='datetime')  
    #Convert timestamp format to datetime for data analysis               
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.drop(columns=['d_'])
    df = df.drop(columns=['created'])
    df = df.drop(columns=['permalink'])
    print(df.columns)
    # Remove special character in df
    df['body'] = df['body'].apply(lambda x: html.unescape(x))
    # Save data to csv
    df.to_csv(f'dataset_comments_lockdown_sg.csv', sep=';', # Save the dataset on a csv file for future analysis
            header=True, index=False, columns=['author','body','id','datetime'])

if __name__== "__main__" :
    main()