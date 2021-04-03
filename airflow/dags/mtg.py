from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
#from airflow.operators.hive_operator import HiveOperator

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

# Create directories

hdfs_create_cards_raw_dir = HdfsMkdirFileOperator(
    task_id='hdfs_mkdir_raw_cards',
    directory='/user/hadoop/mtg/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}'\
              '/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}'\
              '/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_create_cards_final_dir = HdfsMkdirFileOperator(
    task_id='hdfs_mkdir_final_cards',
    directory='/user/hadoop/mtg/final/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}'\
              '/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}'\
              '/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# PySpark

pyspark_download_cards = SparkSubmitOperator(
    task_id='pyspark_download_cards',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_download_cards.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_download_cards',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}',
                      '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}',
                      '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}'],
    conf={
        'spark.rpc.message.maxSize': '256',
        'spark.driver.memory' : '2g',
    },
    dag = dag
)

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

pyspark_export_cards = SparkSubmitOperator(
    task_id='pyspark_export_cards_to_mongodb',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_export_cards.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_export_cards_to_mongodb',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}',
                      '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}',
                      '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}'],
    dag = dag
)

# Airflow

hdfs_create_cards_raw_dir >> hdfs_create_cards_final_dir >> \
    pyspark_download_cards >> pyspark_format_cards >> \
    pyspark_export_cards
