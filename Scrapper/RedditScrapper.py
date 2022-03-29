# Create a web scrapper for reddit
from cgitb import reset
import praw
import pandas as pd
from praw.models import MoreComments
# API key
# reddit = praw.Reddit(client_id='<YOUR CLIENT ID>', client_secret='<YOUR CLIENT SECRET>', user_agent='<YOUR USER AGENT>')
# To find your user agent, go to https://www.reddit.com/prefs/apps/
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
    
    
    
links = ["https://www.reddit.com/r/singapore/comments/tq3kf7/will_people_go_maskoff_outdoors_when_new_covid19/" ,
         "https://www.reddit.com/r/singapore/comments/tqroxy/many_in_singapore_still_donning_masks_outdoors_on/" ,
         'https://www.reddit.com/r/singapore/comments/tniw9g/will_you_wear_a_mask_outside_after_29th_march_why/',
         'https://www.reddit.com/r/singapore/comments/tng1dx/ministry_of_transport_maskwearing_requirement/',
         'https://www.reddit.com/r/singapore/comments/tlytr0/mask_onoff_guidelines_listed_on_the_straits_times/',
         'https://www.reddit.com/r/singapore/comments/tlwbez/group_sizes_to_double_to_10_masks_optional_when/',
         'https://www.reddit.com/r/singapore/comments/tdws0b/mask_never_mbs_badge_lady_seen_unmasked_at_jewel/',
         'https://www.reddit.com/r/singapore/comments/tgy88n/authorities_investigating_badge_lady_who_was/',
         'https://www.reddit.com/r/singapore/comments/tebrnk/streamlined_covid19_measures_begin_from/',
         'https://www.reddit.com/r/singapore/comments/t9eoty/badge_lady_allegedly_spotted_walking_around/'
         ]

df = searchComments(links)
#Save data to csv
df.to_csv(f'dataset_mask_comments.csv', sep=';' , header=True, index=False, columns=['id','comments'])
