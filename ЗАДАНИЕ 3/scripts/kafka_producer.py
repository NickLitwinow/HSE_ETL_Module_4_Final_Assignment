import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, rand

def main(args):
    spark = SparkSession.builder \
        .appName("parquet-to-kafka-loop-json") \
        .getOrCreate()

    df = spark.read.parquet(args.source_path).cache()
    total = df.count()
    print(f"LOADED ROWS: {total}")

    while True:
        batch_df = df.orderBy(rand()).limit(100)
        kafka_df = batch_df.select(to_json(struct([col(c) for c in batch_df.columns])).alias("value"))

        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.kafka_brokers) \
            .option("topic", args.kafka_topic) \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
            .option("kafka.sasl.jaas.config",
                    'org.apache.kafka.common.security.scram.ScramLoginModule required '
                    f'username="{args.kafka_user}" '
                    f'password="{args.kafka_password}";') \
            .save()

        print(f"100 MESSAGES SENT TO: {args.kafka_topic}")
        time.sleep(1)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_path', required=True)
    parser.add_argument('--kafka_brokers', required=True)
    parser.add_argument('--kafka_topic', required=True)
    parser.add_argument('--kafka_user', required=True)
    parser.add_argument('--kafka_password', required=True)
    args = parser.parse_args()
    main(args)
