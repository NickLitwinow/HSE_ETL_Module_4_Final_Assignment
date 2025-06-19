import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType

def main(args):
    spark = SparkSession.builder \
        .appName("dataproc-kafka-read-to-postgres") \
        .getOrCreate()

    schema = StructType() \
        .add("msno", StringType()) \
        .add("payment_method_id", IntegerType()) \
        .add("payment_plan_days", IntegerType()) \
        .add("plan_list_price", IntegerType()) \
        .add("actual_amount_paid", IntegerType()) \
        .add("is_auto_renew", BooleanType()) \
        .add("transaction_date", StringType()) \
        .add("membership_expire_date", StringType()) \
        .add("is_cancel", BooleanType())

    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_brokers) \
        .option("subscribe", args.kafka_topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.scram.ScramLoginModule required '
                f'username="{args.kafka_user}" '
                f'password="{args.kafka_password}";') \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
        .withColumn("membership_expire_date", to_date(col("membership_expire_date"), "yyyy-MM-dd"))

    def write_to_postgres(batch_df, batch_id):
        temp_table_name = f"temp_batch_{batch_id}"
        
        batch_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}") \
            .option("dbtable", temp_table_name) \
            .option("user", args.postgres_user) \
            .option("password", args.postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        upsert_sql = f"""
        INSERT INTO {args.postgres_table} (msno, payment_method_id, payment_plan_days, plan_list_price, actual_amount_paid, is_auto_renew, transaction_date, membership_expire_date, is_cancel)
        SELECT msno, payment_method_id, payment_plan_days, plan_list_price, actual_amount_paid, is_auto_renew, transaction_date, membership_expire_date, is_cancel FROM {temp_table_name}
        ON CONFLICT (msno) DO UPDATE SET
            payment_method_id = EXCLUDED.payment_method_id,
            payment_plan_days = EXCLUDED.payment_plan_days,
            plan_list_price = EXCLUDED.plan_list_price,
            actual_amount_paid = EXCLUDED.actual_amount_paid,
            is_auto_renew = EXCLUDED.is_auto_renew,
            transaction_date = EXCLUDED.transaction_date,
            membership_expire_date = EXCLUDED.membership_expire_date,
            is_cancel = EXCLUDED.is_cancel;
        """
        
        spark.read \
             .format("jdbc") \
             .option("url", f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}") \
             .option("user", args.postgres_user) \
             .option("password", args.postgres_password) \
             .option("driver", "org.postgresql.Driver") \
             .option("query", upsert_sql) \
             .load()

    query = parsed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", args.checkpoint_location) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka_brokers', required=True)
    parser.add_argument('--kafka_topic', required=True)
    parser.add_argument('--kafka_user', required=True)
    parser.add_argument('--kafka_password', required=True)
    parser.add_argument('--postgres_host', required=True)
    parser.add_argument('--postgres_port', default="6432")
    parser.add_argument('--postgres_db', required=True)
    parser.add_argument('--postgres_table', required=True)
    parser.add_argument('--postgres_user', required=True)
    parser.add_argument('--postgres_password', required=True)
    parser.add_argument('--checkpoint_location', required=True)
    args = parser.parse_args()
    main(args)
