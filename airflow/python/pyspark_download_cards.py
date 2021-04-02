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
    mtg_cards = {'cards': []}

    response = requests.get('https://api.magicthegathering.io/v1/cards')
    json_raw = response.json()  

    for card in json_raw['cards']:
        mtg_cards['cards'].append(card) 

    next = response.links['next']['url']
    counter = 0
    while True:
        print(next)
        next_response = requests.get(next)
        next_json_raw = next_response.json()

        for next_card in next_json_raw['cards']:
            mtg_cards['cards'].append(next_card)

        if next == response.links['last']['url'] or counter >= 10:
            break
        next = next_response.links['next']['url']
        counter += counter+1

    #for element in mtg_cards['cards']:
    #    print(json.dumps(element))

    #filename = '/home/hadoop/mtg/cards.json'
    #os.makedirs(os.path.dirname(filename), exist_ok=True)
    #with open(filename, 'w') as file:
    #    json.dump(mtg_cards, file)

    # Write cards from HDFS
    mtg_cards_rdd = sc.parallelize(json.dumps(mtg_cards))
    mtg_cards_df = spark.read\
        .option('multiLine', True)\
        .json(mtg_cards_rdd)
    mtg_cards_df.printSchema()
    mtg_cards_df.show()
    mtg_cards_df.write.format('json')\
        .mode('overwrite')\
        .save(f'/user/hadoop/mtg/raw')


if __name__ == '__main__':
    download_cards()
