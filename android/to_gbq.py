from concurrent.futures import ThreadPoolExecutor
import os

import pandas as pd
from pandas_gbq import to_gbq
import google.generativeai as genai

PROJECT = 'site-reporter-436515'
DATASET = 'app_reporter'
DESTINY_TABLE = 'android_reviews'
MARKETS = {
    'pt-br': 17
}

APP_ID = 'br.com.lojasrenner'

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

def generate_sa_prompt(content, language, score):
    return f'''
        Role: sentiment analysis.
        Language: {language}.
        Rating score: {score}.
        Analyze the following review for an Android app.
        Return me the sentiment and the cause.
        Sentiment is one of the following: [POSITIVE, NEGATIVE, NEUTRAL, MIXED, UNRELATED].
        Cause is one of the following: [BUG, UX, PERFORMANCE, OTHER, MULTIPLE, UNRELATED].
        If cause or sentiment are unrelated, return UNRELATED,UNRELATED
        Return me in the following format, no whitespace or markdown: SENTIMENT,CAUSE
        Review: {content}
    '''

def analyze_review(review, language, score):
    prompt = generate_sa_prompt(review, language, score)
    response = model.generate_content(prompt).text
    print(response)
    sentiment, cause = response.split(',')
    return sentiment, cause

def process_review(review_data):
    # Perform sentiment analysis and translation in parallel
    with ThreadPoolExecutor(5) as executor:
        sentiment_future = executor.submit(analyze_review, review_data['content'], review_data['lang'], review_data['score'])
        translation_future = executor.submit(translate_review, review_data['content'])

        sentiment, cause = sentiment_future.result()
        translation = translation_future.result()

    return sentiment, cause, translation

if __name__ == '__main__':
    for market in MARKETS:
        for i in range(1, MARKETS[market] + 1):
            try: 
                df = pd.read_csv(filename:=f'{APP_ID}.{market}({i}).csv')
                print(filename)
                df['id'] = df['app_id']
                df.drop(columns=['app_id'], inplace=True)
                df['created_at'] = pd.to_datetime(df['created_at'], format='%Y-%m-%d %H:%M:%S')
                df['fetched_at'] = pd.to_datetime(df['fetched_at'], format='%Y-%m-%d %H:%M:%S.%f')

                # Parallelize the processing of reviews
                with ThreadPoolExecutor(5) as executor:
                    results = list(executor.map(process_review, [row for _, row in df.iterrows()]))

                # Unzip the results and assign them to the dataframe
                sentiment, cause, en_content = zip(*results)
                df['sentiment'] = sentiment
                df['cause'] = cause
                df['en_content'] = en_content

                df['sentiment'] = df['sentiment'].str.strip()
                df['cause'] = df['cause'].str.strip()

                to_gbq(df, f'{DATASET}.{DESTINY_TABLE}', project_id=PROJECT, if_exists='append')
                os.remove(filename)
            except FileNotFoundError:
                print(f'File {filename} not found')