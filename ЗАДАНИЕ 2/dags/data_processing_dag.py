import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.models import Variable

# Получение параметров из переменных ветродуйки
YC_DP_SSH_PUBLIC_KEY = Variable.get("yc_dp_ssh_public_key")
YC_DP_SUBNET_ID = Variable.get("yc_dp_subnet_id")
YC_DP_SA_ID = Variable.get("yc_dp_sa_id")
YC_BUCKET = Variable.get("yc_s3_bucket")
YC_DP_AZ = Variable.get("yc_dp_zone", default_var='ru-central1-a')

with DAG(
        'DATA_INGEST_V2',
        schedule_interval='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task-v2',
        cluster_name=f'tmp-dp-batch-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения пакетного PySpark-задания',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-hdd',
        masternode_disk_size=32,
        computenode_resource_preset='s2.small',
        computenode_disk_type='network-hdd',
        computenode_disk_size=32,
        computenode_count=1,
        services=['YARN', 'SPARK', 'HDFS'],
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task-v2',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/clean_data.py',
        properties={
            "spark.submit.deployMode": "cluster"
        }
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task-v2',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
