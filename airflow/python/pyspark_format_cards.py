import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import col, expr, explode, concat_ws

def get_args():
    """
    Parses Command Line Args
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

def format_cards():
    """
    Format MTG Cards
    """
    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read raw cards from HDFS
    mtg_cards_df = spark.read.format('json')\
        .load(f'/user/hadoop/mtg/raw/cards_{args.year}-{args.month}-{args.day}.json')

    # Explode the array into single elements
    mtg_cards_exploded_df = mtg_cards_df.select(explode('cards').alias('exploded'))\
        .select('exploded.*')

    # Replace all null values with empty strings
    mtg_cards_renamed_null_df = mtg_cards_exploded_df\
        .na.fill('')

    columns = ['name', 'subtypes', 'text', 'flavor', 'artist']
    reduced_cards = mtg_cards_renamed_null_df.select(*columns)

    flattened_subtypes = reduced_cards.withColumn('subtypes', concat_ws(', ', 'subtypes'))

    # Write data to HDFS
    flattened_subtypes.write.format('json')\
        .mode('overwrite')\
        .save(f'/user/hadoop/mtg/final/cards_{args.year}-{args.month}-{args.day}.json')

if __name__ == '__main__':
    format_cards()
