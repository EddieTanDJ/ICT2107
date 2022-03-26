# Create a web scrapper for reddit
from cgitb import reset
import praw
import pandas as pd
from praw.models import MoreComments
# API key
# reddit = praw.Reddit(client_id='<YOUR CLIENT ID>', client_secret='<YOUR CLIENT SECRET>', user_agent='<YOUR USER AGENT>')
reddit = praw.Reddit(client_id='OEgaclvEQSIiULMThSZuaA', client_secret='16rsNJVn8Bagu8rKJKoGJ-T0bLLZRw', user_agent='Covid Scrapper')


""" 
Search for all comments in a subreddit links using reddit API
"""
def searchComments(links):
    id_list = []
    comments_list = []
    for link in links:
        submission = reddit.submission(url = link)
        print("Retrieving comments from : " + submission.title)
        # Check for comments
        for comment in submission.comments:
            if isinstance(comment, MoreComments):
                continue
            id_list.append(submission.id)
            comments_list.append(comment.body)
    #Remove deleted and remove post
    data = pd.DataFrame(data=zip(id_list, comments_list),columns=["id","comments"])
    indexNames = data[(data.comments == '[removed]') | (data.comments == '[deleted]')].index
    data.drop(indexNames, inplace=True)
    return data
    
    
    
links = ["https://www.reddit.com/r/singapore/comments/gy1wsa/a_rant_on_govts_handling_of_cb_that_i_dont_think/" ,
         "https://www.reddit.com/r/singapore/comments/fvq6io/covid19_predictions_expect_a_sharp_rise_in_cases/" ,
         'https://www.reddit.com/r/singapore/comments/gmnaav/singapore_to_exit_circuit_breaker_on_jun_1/',
         'https://www.reddit.com/r/singapore/comments/g5c007/covid19_circuit_breaker_to_be_extended_by_one/',
         'https://www.reddit.com/r/singapore/comments/fv83dd/rsingapore_april_covid19_and_circuit_breaker/',
         'https://www.reddit.com/r/singapore/comments/g5c645/how_are_you_guys_feeling_with_the_extension_of/',
         'https://www.reddit.com/r/singapore/comments/gzemxz/the_thing_that_bugs_me_the_most_about_circuit/' ,
         'https://www.reddit.com/r/singapore/comments/ge2858/what_is_one_thing_you_learned_about_yourself/',
         'https://www.reddit.com/r/singapore/comments/fxrhe1/coronavirus_stadiums_to_close_circuitbreaker/',
         'https://www.reddit.com/r/singapore/comments/g2bomm/youth_dances_on_lorong_halus_bridge_says_f_the/'
         ]

df = searchComments(links)
#Save data to csv
df.to_csv(f'dataset_lockdown_comments_sg_1.csv', sep=';' , header=True, index=False, columns=['id','comments'])
