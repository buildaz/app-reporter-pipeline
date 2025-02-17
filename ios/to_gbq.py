from concurrent.futures import ThreadPoolExecutor
import os

import pandas as pd
from pandas_gbq import to_gbq
import google.generativeai as genai

PROJECT = 'site-reporter-436515'
DATASET = 'app_reporter'
DESTINY_TABLE = 'ios_reviews'
MARKETS = {
    'br': 4
} 
APP_ID = '567763947'

genai.configure(api_key='INSERT YOUR API KEY HERE')
model = genai.GenerativeModel('gemini-1.5-flash')

def generate_translate_prompt(content):
    return f'''
        Role: translator.

        Target language: English.

        Content for translation: {content}

        Give me only the translated text, nothing else.
    '''

def translate_review(review):
    prompt = generate_translate_prompt(review)
    response = model.generate_content(prompt).text
    print(response)
    return response

def generate_prompt(title, content, score):
    return f'''
        Role: sentiment analysis.
        Analyze the following review for an Android app.
        Return me the sentiment and the cause.
        Sentiment is one of the following: [POSITIVE, NEGATIVE, NEUTRAL, MIXED, UNRELATED].
        Cause is one of the following: [BUG, UX, PERFORMANCE, OTHER, MULTIPLE, UNRELATED].
        If cause or sentiment are unrelated, return UNRELATED,UNRELATED
        Return me in the following format, no whitespace or markdown: SENTIMENT,CAUSE
        Rating: {score}
        Review: 
        {title}
        {content}
    '''

def analyze_review(title, language, score):
    prompt = generate_prompt(title, language, score)
    response = model.generate_content(prompt).text
    print(response)
    sentiment, cause = response.split(',')
    return sentiment, cause

def to_isodate(date):
    prompt = f'''
            Give me this date: {date} in ISO format.
            No markdown or newlines at the end.
    '''
    response = model.generate_content(prompt)
    print(response.text)
    return response.text

def process_review(review_data):
    # Perform sentiment analysis and translation in parallel
    with ThreadPoolExecutor(5) as executor:
        sentiment_future = executor.submit(analyze_review, review_data['title'], review_data['content'], review_data['score'])
        translation_future = executor.submit(translate_review, review_data['content'])
        iso_date_future = executor.submit(to_isodate, review_data['review_date'])

        sentiment, cause = sentiment_future.result()
        translation = translation_future.result()
        iso_date = iso_date_future.result()

    return sentiment, cause, translation, iso_date

if __name__ == '__main__':
    for market in MARKETS:
        for i in range(1, MARKETS[market] + 1):
            try:
                    print(f'{APP_ID}.{market}({i}).csv')
                    df = pd.read_csv(f'{APP_ID}.{market}({i}).csv')
                    df['id'] = df['app_id'].astype(str)
                    df.drop(columns=['app_id'], inplace=True)
                    # df['created_at'] = pd.to_datetime(df['created_at'], format='%Y-%m-%d %H:%M:%S')
                    df['fetched_at'] = pd.to_datetime(df['fetched_at'], format='%Y-%m-%d %H:%M:%S.%f')

                    # Parallelize the processing of reviews
                    with ThreadPoolExecutor(5) as executor:
                        results = list(executor.map(process_review, [row for _, row in df.iterrows()]))

                    # Unzip the results and assign them to the dataframe
                    sentiment, cause, en_content, iso_date = zip(*results)
                    df['sentiment'] = sentiment
                    df['cause'] = cause
                    df['en_content'] = en_content
                    df['created_at'] = iso_date

                    df['sentiment'] = df['sentiment'].str.strip()
                    df['cause'] = df['cause'].str.strip()
                    df['created_at'] = df['created_at'].str.strip()
                    df['created_at'] = pd.to_datetime(df['created_at'], format='%Y-%m-%d')
                    df.drop(columns=['review_date'], inplace=True)

                    to_gbq(df, f'{DATASET}.{DESTINY_TABLE}', project_id=PROJECT, if_exists='append')
                    os.remove(f'{APP_ID}.{market}({i}).csv')
            except FileNotFoundError:
                print(f'File {APP_ID}.{market}({i}).csv not found')