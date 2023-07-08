import airflow
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator


dag_spark = DAG(
    dag_id = 'dag_taxi_trip_analysis',
    start_date=datetime(2021, 2, 1),
    schedule_interval=None,
    description='submit taxi trip analysis spark job in airflow'
)

start = DummyOperator(task_id = 'start', dag=dag_spark)

spark_submit_taxi_trip_analysis = SparkSubmitOperator(
    application='/home/hendri/airflow/spark_code/taxi_trip_analysis.py',
    conn_id='spark-standalone',
    task_id='spark_submit',
    dag=dag_spark
)

end = DummyOperator(task_id = 'end', dag=dag_spark)

# Define task dependencies
start >> spark_submit_taxi_trip_analysis >> end
