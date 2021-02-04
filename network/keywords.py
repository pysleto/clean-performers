"""
This program builds prepares a co-occurence matrix to use in Gephi from a Pitchbook csv file of companies containing
keywords.
https://www.pingshiuanchua.com/blog/post/keyword-network-analysis-with-python-and-gephi?utm_campaign=News&utm_medium=Community&utm_source=DataCamp.com
"""

from pathlib import Path

from config import cfg

from database.scripts.read import get_data_from_csv

from sklearn.feature_extraction.text import CountVectorizer

import pandas as pd


def main():

    # get the company data into a dictionary structure
    with Path(r"V:\Keyword Network Analysis\Company keywords - Battery.csv") as csv_file_path:
        data = get_data_from_csv(csv_file_path)
        companies = data

    keyword_list = [company['Keywords'] for company in companies]

    with Path(r"V:\Keyword Network Analysis\stop_word_list.csv") as csv_file_path:
        data = get_data_from_csv(csv_file_path)
        stop_words = data

    stop_word_list = [stop_word['stop_words'] for stop_word in stop_words]

    vectorizer = CountVectorizer(ngram_range=(1, 1), token_pattern='(?u)[a-zA-Z][a-z ]+', min_df=3,
                                 stop_words=stop_word_list)  # You can define your own parameters
    X = vectorizer.fit_transform(keyword_list)

    # print(vectorizer.get_feature_names())

    Xc = (X.T * X)  # This is the matrix manipulation step
    Xc.setdiag(0)  # We set the diagonals to be zeroes as it's pointless to be 1

    names = vectorizer.get_feature_names()  # This are the entity names (i.e. keywords)
    counts = X.toarray().sum(axis=0)
    df = pd.DataFrame(data=Xc.toarray(), columns=names, index=names)
    df.to_csv(r"V:\Keyword Network Analysis\to_gephi.csv", sep=',')

    # df = pd.DataFrame(data=zip(names, counts), columns=['name', 'count'])
    # df.to_csv(r"M:\Keyword Network Analysis\output.csv", sep=',', index=False)


if __name__ == "__main__":
    main()
