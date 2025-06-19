import uuid
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.yandex.operators.yandexcloud_dataproc import DataprocCreatePysparkJobOperator

# Получение параметров из ветродуйки
YC_DP_CLUSTER_ID = Variable.get("yc_dp_streaming_cluster_id")
YC_BUCKET = Variable.get("yc_s3_bucket")
KAFKA_BROKERS = Variable.get("yc_kafka_brokers")
KAFKA_TOPIC = Variable.get("yc_kafka_topic")
KAFKA_USER = Variable.get("yc_kafka_user")
KAFKA_PASSWORD = Variable.get("yc_kafka_password")
POSTGRES_HOST = Variable.get("yc_postgres_host")
POSTGRES_DB = Variable.get("yc_postgres_db")
POSTGRES_TABLE = Variable.get("yc_postgres_table")
POSTGRES_USER = Variable.get("yc_postgres_user")
POSTGRES_PASSWORD = Variable.get("yc_postgres_password")

# Аргументы для PySpark скриптов
producer_args = [
    f"--source_path=s3a://{YC_BUCKET}/transactions_v2_clean.parquet",
    f"--kafka_brokers={KAFKA_BROKERS}",
    f"--kafka_topic={KAFKA_TOPIC}",
    f"--kafka_user={KAFKA_USER}",
    f"--kafka_password={KAFKA_PASSWORD}",
]

consumer_args = [
    f"--kafka_brokers={KAFKA_BROKERS}",
    f"--kafka_topic={KAFKA_TOPIC}",
    f"--kafka_user={KAFKA_USER}",
    f"--kafka_password={KAFKA_PASSWORD}",
    f"--postgres_host={POSTGRES_HOST}",
    f"--postgres_db={POSTGRES_DB}",
    f"--postgres_table={POSTGRES_TABLE}",
    f"--postgres_user={POSTGRES_USER}",
    f"--postgres_password={POSTGRES_PASSWORD}",
    f"--checkpoint_location=s3a://{YC_BUCKET}/checkpoints/kafka-postgres-checkpoint",
]

with DAG(
        'STREAMING_JOBS',
        schedule_interval=None,
        tags=['data-streaming-and-kafka'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as streaming_dag:
    start_kafka_producer_job = DataprocCreatePysparkJobOperator(
        task_id='dp-kafka-producer-job',
        cluster_id=YC_DP_CLUSTER_ID,
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/kafka_producer.py',
        args=producer_args,
    )

    start_kafka_consumer_job = DataprocCreatePysparkJobOperator(
        task_id='dp-kafka-consumer-job',
        cluster_id=YC_DP_CLUSTER_ID,
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/kafka_consumer_to_postgres.py',
        args=consumer_args,
        properties={
            # Добавляем пакет PostgreSQL для Spark
            "spark.jars.packages": "org.postgresql:postgresql:42.2.25"
        }
    )

    start_kafka_producer_job >> start_kafka_consumer_job 