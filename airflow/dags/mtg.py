from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator

# Setup DAG

args = {
    'owner': 'airflow'
}

dag = DAG('MTG', default_args=args, description='Magic: The Gathering - Import Cards',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

# SQL Setup

hiveSQL_create_table_cards='''
CREATE EXTERNAL TABLE IF NOT EXISTS cards(
	card_id BIGINT DEFAULT SURROGATE_KEY(),
    name STRING,
    subtype BIGINT,
    text STRING,
    flavor STRING,
    artist STRING
) COMMENT 'MTG Cards' PARTITIONED BY (partition_year int, partition_month int, partition_day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/title_ratings'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

hiveSQL_add_partition_cards='''
ALTER TABLE cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/imdb/title_ratings/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/';
'''

# Create direcctories and download data

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='mtg',
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/mtg',
    pattern='*',
    dag=dag,
)

download_cards = HttpDownloadOperator(
    task_id='download_cards',
    download_uri='https://api.magicthegathering.io/v1/cards',
    save_to='/home/airflow/mtg/cards_{{ ds }}.json',
    dag=dag,
)

# Put data to HDFS

hdfs_create_cards_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_cards_dir',
    directory='/user/hadoop/mtg/cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_cards = HdfsPutFileOperator(
    task_id='upload_cards_to_hdfs',
    local_file='/home/airflow/mtg/cards_{{ ds }}.tsv',
    remote_file='/user/hadoop/mtg/cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/title.ratings_{{ ds }}.tsv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# Create Hive table

HiveTable_create_cards = HiveOperator(
    task_id='create_cards_table',
    hql=hiveSQL_create_table_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

HiveTable_addPartition_cards = HiveOperator(
    task_id='add_partition_cards_table',
    hql=hiveSQL_add_partition_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

dummy_op = DummyOperator(
    task_id='dummy', 
    dag=dag)

create_local_import_dir >> clear_local_import_dir >> download_cards >> \
        hdfs_create_cards_partition_dir >> hdfs_put_cards >> \
        HiveTable_create_cards >> HiveTable_addPartition_cards >> \
        dummy_op
