import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import desc

def get_args():
    """
    Parses Command Line Args
    """
    parser = argparse.ArgumentParser(description='Some Basic Spark Job doing some stuff on IMDb data stored within HDFS.')
    parser.add_argument('--year',
                        help='Partion Year To Process, e.g. 2019',
                        required=True, type=str)
    parser.add_argument('--month',
                        help='Partion Month To Process, e.g. 10',
                        required=True, type=str)
    parser.add_argument('--day',
                        help='Partion Day To Process, e.g. 31',
                        required=True, type=str)
    parser.add_argument('--hdfs_source_dir',
                        help='HDFS source directory, e.g. /user/hadoop/mtg/raw',
                        required=True, type=str)
    parser.add_argument('--hdfs_target_dir',
                        help='HDFS target directory, e.g. /user/hadoop/mtg/final',
                        required=True, type=str)
    
    return parser.parse_args()

if __name__ == '__main__':
    """
    Main Function
    """

    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Initialize Hive Context
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    # Read raw cards from HDFS
    mtg_cards_raw_dataframe = spark.read.format('json')\
        .load(args.hdfs_source_dir + f'/cards_{args.year}-{args.month}-{args.day}.json')

    mtg_cards_raw_dataframe.printSchema()
    mtg_cards_raw_dataframe.registerTempTable('cards')
    sqlContext
    
    # Write data to HDFS
    mtg_cards_raw_dataframe.write.format('json').\
            mode('overwrite').\
            save(args.hdfs_target_dir)
