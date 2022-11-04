import random
import os

from dotenv import load_dotenv
import tweepy
from elastic_enterprise_search import AppSearch

load_dotenv()

app_search = AppSearch(
    os.environ['APP_SEARCH_BASE_URL_FN'],
    os.environ['APP_SEARCH_API_KEY'],
)

client = tweepy.Client(os.environ['TWITTER_BEARER_TOKEN'])
    
def insert_tweets(may_skip, user, tweets):
    
    for tweet in tweets.data:
        
        # Randomly verify if the document exists
        # If so, this user will be skipped
        if may_skip and random.randint(0,9) <= 2:
            try:
                res = app_search.get_documents(
                    engine_name=os.environ['APP_SEARCH_ENGINE_NAME'],
                    document_ids=[f'tweet-{tweet["id"]}']
                )
                
                if res[0] is not None:
                    return False
            except Exception as e:
                print(e)
                
        
        url = f'https://twitter.com/{user.username}/status/{tweet["id"]}'
        print(user.username)
        print(tweet['created_at'])
        
        title = f"{user.name} on Twitter - {str(tweet)[0:50]}"
        
        if (len(str(tweet)) > 50):
            title = f"{title}..."
        
        res = app_search.index_documents(
            engine_name=os.environ['APP_SEARCH_ENGINE_NAME'],
            documents=[{
                "additional_urls": url,
                "body_content": str(tweet),
                "domains": ["https://twitter.com"],
                "title": title,
                "url": url,
                "url_scheme": "https",
                "id": f'tweet-{tweet["id"]}',
                "created_at": tweet['created_at'],
                "username": user.username,
                "user_id": user.id
            }]
        )
        
        print(res)
        
    return True

def insert_user_tweets(may_skip, user):
    
    tweets = client.get_users_tweets(id=user.id, tweet_fields=['created_at'], max_results=100)

    while True:
        
        if not tweets.data:
            break
        
        if not insert_tweets(may_skip, user, tweets):
            print(f'{user.username} skipped')
            break
        
        try:
            next_token = tweets.meta['next_token']
        except KeyError:
            break
        
        tweets = client.get_users_tweets(id=user.id, pagination_token=next_token, tweet_fields=['created_at'], max_results=100)
        
    
if __name__ == "__main__":
    usernames = ['pwuille', 'murchandamus', 'notgrubles', 'bitstein', 'roasbeef']
    users = client.get_users(usernames=usernames)

    if not users.data:
        print("client.get_users() didn't return anything.")
        exit(0)
     
    for user in users.data:
        insert_user_tweets(True, user)