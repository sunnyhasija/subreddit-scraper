#import all the libraries
from datetime import datetime
from configparser import ConfigParser

import json #to parse the output
import praw #python reddit api wrapper
import requests
import time #for system time

try:
    from unidecode import unidecode
except ImportError:
    import sys
    #dfdgfg
    print("Install unidecode with pip")
    print("pip install --user unidecode")
    sys.exit(1)

#read the ini file for the Oauth and the Time stuff
config = ConfigParser()
config.read("reddit_config.ini")

CLIENT_ID = config.get("main", "client-id")
CLIENT_SECRET = config.get("main", "client-secret")
SUBREDDITS = json.loads(config.get("main", "subreddits"))


#commeting out getting time from ini - this will allow to use crontab.
#The idea here is that we are going to run the file once a day, and thus, we are going to be able to set the time in a relative fashion.
#If you want to explicitly pass the start time and end time to the script, uncomment the two lines below, and comment out the lines below those two.
#START_TIME = int(config.get("time", "start"))
#ACTUAL_END_TIME = int(config.get("time", "end"))

START_TIME = int(time.time() - 172800) # comment this line if you want to explicitly give start time
ACTUAL_END_TIME= int(START_TIME + 86400) # comment this line if you want to explicitly give end time.
print(f"Scraping start time is: {START_TIME}")
print(f"Scraping end time is: {ACTUAL_END_TIME}")
session = requests.Session()

#scrape the data 

def scrape_data(subreddit):
    '''
    pushshift only gives 1000 posts at a time, so we hack it
    to send us 1000 posts, and then use the created_time for
    the last post as the start time for the next iteration
    and get as many posts as we can.
    '''
    start_time = START_TIME
    end_time = ACTUAL_END_TIME


    post_id_list = []

    #loop through pushshift to get all the posts in a subreddit. 
    while start_time <= end_time:
        API_URL = "https://api.pushshift.io/reddit/submission/search/?after={0}&before={1}&sort_type=created_utc&sort=asc&subreddit={2}".format(
            start_time, end_time, subreddit
        )
        response = session.get(API_URL)

        if response.ok:
            data = response.json().get("data", [])

        try:
            start_time = data[-1].get("created_utc")
            post_id_list.extend([post.get("id") for post in data])
        except IndexError:
            break
    #get the comments from reddit using praw
    reddit = praw.Reddit(user_agent="Comment Analyzer", client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    #create container for the submissions and comments
    submission_map = {}

    for post_id in post_id_list:
        submission = reddit.submission(id=post_id) #pass submission post id
        submission.comments.replace_more(limit=None) #get all the comments
        submission_comments = {} #initiate blank submission comment frame
        comment_queue = submission.comments[:]
        while comment_queue: #iterate over comment comment_queue
            comment = comment_queue.pop(0)
            submission_comments[comment.id] = {
                "comment_body": unidecode(comment.body), #strip emojis
                "comment_score": comment.score,
                "comment_parent_id": comment.parent_id,
                "comment_is_root": comment.is_root,
                "comment_author": comment.author.name if comment.author else "[deleted]",
                "comment_upvotes": comment.ups,
                "comment_downvotes": comment.downs,
                "comment_total_awards_received": comment.total_awards_received,
                "comment_awardings": comment.all_awardings,
                "comment_gildings": comment.gildings,
                "comment_created_at": datetime.utcfromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S')
            }
            comment_queue.extend(comment.replies) #append comments to include replies? 
        #get post level data
        submission_map[submission.id] = {
            "comments": submission_comments,
            "is_submission": True,
            "score": submission.score,
            "title": unidecode(submission.title), #strip emojis from the text
            "url": submission.shortlink,
            "author": submission.author.name if submission.author else "[deleted]",
            "total_awards_received": submission.total_awards_received,
            "awardings": submission.all_awardings,
            "gildings": submission.gildings,
            "upvotes": submission.ups,
            "downvotes": submission.downs,
            "created_at": datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S') #save time of creation in human readable format
        }

    logfile = "reddit_{0}_comments_{1}.log".format(subreddit, time.strftime('%Y%m%d-%H%M%S')) #save file append human readable timestamp
    with open(logfile, "w") as log:
        log.write(json.dumps(submission_map, indent=4, sort_keys=True))
        log.flush()


for subreddit in SUBREDDITS: #iterate over subreddit list
    scrape_data(subreddit)
