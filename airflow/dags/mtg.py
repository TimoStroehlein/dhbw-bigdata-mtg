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

# https://community.cloudera.com/t5/Support-Questions/create-hive-table-with-this-json-format/td-p/162384
# https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-get_json_object
# https://github.com/rcongiu/Hive-JSON-Serde
# http://thornydev.blogspot.com/2013/07/querying-json-records-via-hive.html
# https://community.cloudera.com/t5/Support-Questions/org-apache-hive-hcatalog-data-JsonSerDe-not-found/td-p/191730

# Setup DAG

args = {
    'owner': 'airflow'
}

dag = DAG('MTG', default_args=args, description='Magic: The Gathering - Import Cards',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

# SQL Setup

hiveSQL_add_json_serde='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
'''

hiveSQL_create_table_cards='''
DROP TABLE IF EXISTS cards;
CREATE EXTERNAL TABLE IF NOT EXISTS cards(
	name STRING,
    subtype BIGINT,
    text STRING,
    flavor STRING,
    artist STRING
) COMMENT 'MTG Cards' PARTITIONED BY (
    partition_year int, partition_month int, partition_day int
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/final';
'''

hiveSQL_add_partition_cards='''
ALTER TABLE cards
ADD IF NOT EXISTS partition(
    partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}},
    partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}},
    partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/final/partitioned';
'''

hiveSQL_load_data_into_cards='''
LOAD DATA INPATH '/user/hadoop/mtg/raw/cards_{{ ds }}.json' INTO TABLE cards;
SELECT * FROM cards;
'''

# Add json serde

HiveTable_add_json_serde=HiveOperator(
    task_id='add_json_serde',
    hql=hiveSQL_add_json_serde,
    hive_cli_conn_id='beeline',
    dag=dag)

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
    task_id='hdfs_mkdir_raw_cards',
    directory='/user/hadoop/mtg/raw',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_cards = HdfsPutFileOperator(
    task_id='hdfs_upload_raw_cards_to_hdfs',
    local_file='/home/airflow/mtg/cards_{{ ds }}.json',
    remote_file='/user/hadoop/mtg/raw/cards_{{ ds }}.json',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# Dummy

dummy_op = DummyOperator(
    task_id='dummy', 
    dag=dag)

# PySpark

pyspark_format_cards = SparkSubmitOperator(
    task_id='pyspark_format_cards',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_format_cards.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_format_cards',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}',
                      '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}',
                      '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}'],
    dag = dag
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

HiveTable_load_data_into_cards = HiveOperator(
    task_id='load_data_into_cards_table',
    hql=hiveSQL_load_data_into_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

# Airflow

HiveTable_add_json_serde >> \
        create_local_import_dir >> clear_local_import_dir >> download_cards >> \
        hdfs_create_cards_partition_dir >> hdfs_put_cards >> \
        dummy_op
dummy_op >> \
        HiveTable_create_cards >> HiveTable_addPartition_cards >> \
        HiveTable_load_data_into_cards
dummy_op >> \
        pyspark_format_cards
