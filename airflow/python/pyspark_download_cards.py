import pyspark
import argparse
import json
import requests
import os

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, expr, explode, concat_ws
from pymongo import MongoClient


def get_args():
    """
    Parses command line args.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--year',
                        help='Partion Year To Process',
                        required=True, type=str)
    parser.add_argument('--month',
                        help='Partion Month To Process',
                        required=True, type=str)
    parser.add_argument('--day',
                        help='Partion Day To Process',
                        required=True, type=str)
    
    return parser.parse_args()


def download_cards():
    """
    Export final cards to MongoDB.
    """
    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Download cards from the MTG API
    mtg_cards = []

    # Make a request against the MTG API and get the json response
    response = requests.get('https://api.magicthegathering.io/v1/cards')
    json_raw = response.json()
    mtg_cards.append(json.dumps(json_raw)) 

    # Get the next page of the MTG API
    next = response.links['next']['url']
    next_count = int(response.headers.get('count'))
    next_total_count = int(response.headers.get('total-count'))

    while True:
        # Calculate the completion and print the percentage
        percentage = round((next_count / next_total_count) * 100, 1)
        print(f'{percentage}%')

        # Get the next page
        next_response = requests.get(next)
        next_json_raw = next_response.json()
        mtg_cards.append(json.dumps(next_json_raw))

        # When the last page has been reached, stop
        if 'next' not in next_response.links:
            print('Successfully downloaded all data.')
            break

        # Get the next page of the MTG API
        next = next_response.links['next']['url']
        next_count += int(next_response.headers.get('count'))
        next_total_count = int(next_response.headers.get('total-count'))

    # Convert json with cards to dataframe
    mtg_cards_rdd = sc.parallelize(mtg_cards)
    mtg_cards_df = spark.read\
        .option('multiline','true')\
        .json(mtg_cards_rdd)

    # Write dataframe with cards to hdfs
    mtg_cards_df.write.format('json')\
        .mode('overwrite')\
        .save(f'/user/hadoop/mtg/raw/{args.year}/{args.month}/{args.day}')


if __name__ == '__main__':
    download_cards()
