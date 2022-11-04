from datetime import datetime
import os
import sqlite3
import logging

import praw
from dotenv import load_dotenv
from elastic_enterprise_search import AppSearch

load_dotenv()

app_search = AppSearch(
    os.environ['APP_SEARCH_BASE_URL_FN'],
    os.environ['APP_SEARCH_API_KEY'],
)

reddit = praw.Reddit(
    client_id=os.environ['REDDIT_CLIENT_ID'],
    client_secret=os.environ['REDDIT_CLIENT_SECRET'],
    user_agent=os.environ['REDDIT_USER_AGENT'],
)

logging.basicConfig(filename='reddit-crawler.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

def insert_submission(item):
    
    if not item.selftext:
        print(f"submission {item.id} skipped. Url: https://www.reddit.com{item.permalink}")
        return True
    
    try:
        
        document = {
                "additional_urls": f"https://www.reddit.com{item.permalink}",
                "body_content": item.selftext,
                "domains": ["https://www.reddit.com"],
                "title": item.title,
                "url": f"https://www.reddit.com{item.permalink}",
                "url_scheme": "https",
                "id": f'reddit-{item.subreddit}-{item.id}',
                "created_at": datetime.utcfromtimestamp(item.created_utc),
                "reddit_data_type": "submission"
            }
        
        # apparently when the user is deleted the "author" field is NoneType
        if item.author:
            try:
                document["redditor_username"] = item.author.name
                document["redditor_id"] = item.author.id
            # Even it the author exists, sometimes the error: "AttributeError: 'Redditor' object has no attribute 'id'" happens
            except AttributeError:
                pass
            
        
        res = app_search.index_documents(
            engine_name=os.environ['APP_SEARCH_ENGINE_NAME'],
            documents=[document]
        )
        print(f'submission: {res}')
        logging.info(f'submission: {res}')
        return True
    except Exception as e:
        print(e)
        print(f"submission.id: {item.id}")
        print("---")
        logging.error("Exception occurred while inserting submission", exc_info=True)
        logging.error(f"submission.id: {item.id}")
        logging.error(f"comment.permalink: https://www.reddit.com{item.permalink}")
        logging.error("---")
        return False
    
def insert_comment(item):
    try:
        title = f"{item.author} on Reddit - {item.body[0:50]}"
    
        if (len(item.body) > 50):
            title = f"{title}..."
            
        document = {
                "additional_urls": f"https://www.reddit.com{item.permalink}",
                "body_content": item.body,
                "domains": ["https://www.reddit.com"],
                "title": title,
                "url": f"https://www.reddit.com{item.permalink}",
                "url_scheme": "https",
                "id": f'reddit-{item.subreddit}-{item.id}',
                "created_at": datetime.utcfromtimestamp(item.created_utc),
                "reddit_data_type": "comment",
                "submission_id": item.link_id
            }
        
        # apparently when the user is deleted the "author" field is NoneType
        if item.author:
            try:
                document["redditor_username"] = item.author.name
                document["redditor_id"] = item.author.id
            # Even it the author exists, sometimes the error: "AttributeError: 'Redditor' object has no attribute 'id'" happens
            except AttributeError:
                pass
            
        res = app_search.index_documents(
            engine_name=os.environ['APP_SEARCH_ENGINE_NAME'],
            documents=[document]
        )
        print(f'comment: {res}')
        logging.info(f'comment: {res}')
        return True
    except Exception as e:
        print(e)
        print(f"comment.id: {item.id}")
        print(f"comment.link_id: {item.link_id}")
        print("---")
        logging.error("Exception occurred while inserting comment", exc_info=True)
        logging.error(f"comment.id: {item.id}")
        logging.error(f"comment.link_id: {item.link_id}")
        logging.error(f"comment.permalink: https://www.reddit.com{item.permalink}")
        logging.error("---")
        return False

def try_insert_item(item, insert_function):
    
    attempts = 0
    inserted = False
        
    while attempts <= 10:
        attempts = attempts + 1
        inserted = insert_function(item)
        if inserted:
            break
        
    return inserted

def run_redditor(connection, redditor, allowed_channels):
    
    cursor = connection.cursor()
    row = cursor.execute(
        "SELECT submission_last_date, submission_last_result, comment_last_date, comment_last_result " +
        "FROM user_data " +
        "WHERE username = ?", (redditor, )).fetchall()
    cursor.close()
        
    user_submission_last_date = -1
    user_submission_last_result = False
    user_comment_last_date = -1
    user_comment_last_result = False
    
    if row:
        user_submission_last_date = row[0][0]
        user_submission_last_result = bool(row[0][1])
        user_comment_last_date = row[0][2]
        user_comment_last_result = bool(row[0][3])
        
    print(f"user_submission_last_date: {user_submission_last_date}")
    print(f"user_submission_last_result: {user_submission_last_result}")
    print(f"user_comment_last_date: {user_comment_last_date}")
    print(f"user_comment_last_result: {user_comment_last_result}")
    
    inserted_submission = True
    current_submission_last_date = user_submission_last_date
    
    inserted_comment = True
    current_comment_last_date = user_comment_last_result
    
    for submission in reddit.redditor(redditor).submissions.new(limit=None):
        
        subreddit_name = str(submission.subreddit)
                    
        if subreddit_name not in allowed_channels and "btc" not in subreddit_name.lower() and "bitcoin" not in subreddit_name.lower():
            print(f"{redditor} submission.subreddit: {submission.subreddit} skipping")
            logging.error(f"{redditor} submission.subreddit: {submission.subreddit} skipping")
            continue
        else:
            print(f"{redditor} submission.subreddit: {submission.subreddit} process")
        
        if submission.created_utc <= user_submission_last_date and user_submission_last_result:
            break
        
        inserted_submission = try_insert_item(submission, insert_submission)
            
        if not inserted_submission:
            break
        
        if current_submission_last_date < submission.created_utc:
            current_submission_last_date = submission.created_utc
            
    print("------")
        
    for comment in reddit.redditor(redditor).comments.new(limit=None):
        
        subreddit_name = str(comment.subreddit)
        
        if subreddit_name not in allowed_channels and "btc" not in subreddit_name.lower() and "bitcoin" not in subreddit_name.lower():
            print(f"{redditor} comment.subreddit: {comment.subreddit} skipping")
            logging.error(f"{redditor} comment.subreddit: {comment.subreddit} skipping")
            continue
        else:
            print(f"{redditor} comment.subreddit: {comment.subreddit} process")
        
        if comment.created_utc <= user_comment_last_date and user_comment_last_result:
            break
        
        inserted_comment = try_insert_item(comment, insert_comment)
        
        if not inserted_comment:
            break
        
        if current_comment_last_date < comment.created_utc:
            current_comment_last_date = comment.created_utc
        
    data = {}
    data['username'] = redditor
    data['submission_last_date'] = current_submission_last_date
    data['submission_last_result'] = inserted_submission
    
    data['comment_last_date'] = current_comment_last_date
    data['comment_last_result'] = inserted_comment

    save_user_data(connection, data)

def run_subreddit(connection, subreddit_name):
    
    cursor = connection.cursor()
    row = cursor.execute(
        "SELECT subreddit_last_date, subreddit_last_result " +
        "FROM subreddit_data " +
        "WHERE subreddit_name = ?", (subreddit_name, )).fetchall()
    cursor.close()
        
    subreddit_last_date = -1
    subreddit_last_result = False
    
    if row:
        subreddit_last_date = row[0][0]
        subreddit_last_result = bool(row[0][1])
        
    print(f"subreddit_last_date: {subreddit_last_date}")
    print(f"subreddit_last_result: {subreddit_last_result}")
    
    inserted = True
    current_last_date = subreddit_last_date
    
    for submission in reddit.subreddit(subreddit_name).new(limit=None):
        
        if submission.created_utc <= subreddit_last_date and subreddit_last_result:
            break
        
        inserted = try_insert_item(submission, insert_submission)
        
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            inserted = try_insert_item(comment, insert_comment)
            
        if not inserted:
            break
        
        if current_last_date < submission.created_utc:
            current_last_date = submission.created_utc
            
    data = {}
    data['subreddit_name'] = subreddit_name
    data['subreddit_last_date'] = current_last_date
    data['subreddit_last_result'] = inserted

    save_subreddit_data(connection, data)

def create_subreddit_table(connection):
    cursor = connection.cursor()
    
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS subreddit_data (" +
            "subreddit_name TEXT, " +
            "subreddit_last_date INTEGER, " + 
            "subreddit_last_result INTEGER)")
    
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_subreddit_name " +
        "ON subreddit_data(subreddit_name);")
    
    connection.commit()
    
    cursor.close()
    
def save_subreddit_data(cursor, data):
    cursor = connection.cursor()
    
    cursor.execute("DELETE FROM subreddit_data WHERE subreddit_name = ?", (data['subreddit_name'],) )
    
    cursor.execute("INSERT INTO subreddit_data VALUES (?,?,?)", (
        data['subreddit_name'], data['subreddit_last_date'], data['subreddit_last_result']))
    
    connection.commit()
    
    cursor.close()

def create_user_table(connection):
    cursor = connection.cursor()
    
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS user_data (" +
            "username TEXT, " +
            "submission_last_date INTEGER, " + 
            "submission_last_result INTEGER, " + 
            "comment_last_date INTEGER, " + 
            "comment_last_result INTEGER)")
    
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_username " +
        "ON user_data(username);")
    
    connection.commit()
    
    cursor.close()


def save_user_data(cursor, data):
    cursor = connection.cursor()
    
    cursor.execute("DELETE FROM user_data WHERE username = ?", (data['username'],) )
    
    cursor.execute("INSERT INTO user_data VALUES (?,?,?,?,?)", (
        data['username'], data['submission_last_date'], data['submission_last_result'], 
        data['comment_last_date'], data['comment_last_result']))
    
    connection.commit()
    
    cursor.close()

if __name__ == "__main__":
    logging.info('Script started.')
    
    subreddit_channels = ["Bitcoin", "lightningnetwork", "Electrum", "cryptography", "privacy", "TREZOR", "ledgerwallet", 
                          "joinmarket", "Monero"]
    
    connection = sqlite3.connect("crawler_data.sqlite")
    with connection:
        create_user_table(connection)
        for redditor in ["statoshi", "adam3us", "belcher_", "mperklin", "luke-jr", "theymos", "GibbsSamplePlatter", 
                         "bitusher", "RustyReddit", "-johoe", "nullc", "rnvk", "achow101", "killerstorm",
                         "coblee", "maaku7", "pwuille", "renepickhardt", "fluffyponyza", "JeremyBTC"]:
            run_redditor(connection, redditor, subreddit_channels)
            
        create_subreddit_table(connection)
        for subreddit_name in subreddit_channels:
            run_subreddit(connection, subreddit_name)