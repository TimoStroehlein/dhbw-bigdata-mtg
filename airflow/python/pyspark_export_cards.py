import pyspark
import argparse
import json

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


def export_cards():
    """
    Export final cards to MongoDB.
    """
    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read final cards as json from HDFS
    mtg_cards_df = spark.read.json(f'/user/hadoop/mtg/final/{args.year}/{args.month}/{args.day}')

    # Convert dataframe to json
    mtg_cards_json = mtg_cards_df.toJSON().map(lambda j: json.loads(j)).collect()

    # Connect to MongoDB
    cards_collection = mongodb_cards_collection()
    cards_collection.remove({}) # Remove all documents from the collection
    cards_collection.insert_many(mtg_cards_json)


def mongodb_cards_collection():
    """
    Connect to MongoDB.
    :return: Cards collection of MongoDB.
    """
    client = MongoClient(host='mongo', port=27017, serverSelectionTimeoutMS=5000)
    db = client['mtg']
    db.authenticate('dev', 'dev')
    return db['cards']


if __name__ == '__main__':
    export_cards()
