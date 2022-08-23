import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, SparkSession, functions as F

from delta.tables import DeltaTable

ORIGINAL_DELTA_PATH = 's3://bucket/delta_lake_cdc/example'

spark = SparkSession.builder \
    .appName('delta_lake_cdc')\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .getOrCreate()

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

KAFKA_CONFIG = {
    'kafka.bootstrap.servers': 'b-1.kafka-cluster-dev.....',
    'kafka.ssl.truststore.location': '/usr/lib/jvm/jre/lib/security/cacerts',
    'kafka.security.protocol': 'SSL',
    'kafka.ssl.protocol': 'SSL',
    'assign': '{"mysql-test-table": [0,1] }',
    'groupIdPrefix': 'kafka-to-delta-lake-cdc-streaming',
    'minPartitions': 200,
    'maxOffsetsPerTrigger': 60000*30*60, ## QPS Calculate
    'startingOffsets': 'latest',
    'failOnDataLoss': True,
    'kafka.fetch.min.bytes': 1048576,
    'kafka.fetch.max.wait.ms': 10000
}

def foreach_batch_cdc_func(mirco_batch_df, batchId):
  is_delta_lake = DeltaTable.isDeltaTable(spark, ORIGINAL_DELTA_PATH)
  
  if not is_delta_lake:
    mirco_batch_df.write \
      .format('delta') \
      .mode('append') \
      .partitionBy('dt', 'hour', 'minute') \
      .option('mergeSchema', 'true') \
      .save(ORIGINAL_DELTA_PATH)
  else:
    original_delta_df = DeltaTable.forPath(spark, ORIGINAL_DELTA_PATH)
    
    original_delta_df.alias('old_df')\
        .merge(mirco_batch_df.alias('new_df'), "old_df.id = new_df.id")\
        .whenMatchedDelete(condition = "new_df.op = 'd'")\
        .whenMatchedUpdate(
            condition = "new_df.op = 'u'",
            set = {"name": F.col("new_df.name")}
        )\
        .whenNotMatchedInsertAll()\
        .execute()
    
## Using UpdateAll for merge into ##
#     original_delta_df.alias('old_df')\
#         .merge(mirco_batch_df.alias('new_df'), "old_df.id = new_df.id")\
#         .whenMatchedDelete(condition = "new_df.op = 'd'")\
#         .whenMatchedUpdateAll(
#             condition = "new_df.op = 'u'"
#         )\
#         .whenNotMatchedInsertAll()\
#         .execute()

## Specific date period to execute cdc ##
#     original_delta_df.alias('old_df')\
#         .merge(mirco_batch_df.alias('new_df'), "old_df.id = new_df.id")\
#         .whenMatchedDelete(condition = "new_df.op = 'd'")\
#         .whenMatchedUpdateAll(
#             condition = "new_df.op = 'u'"
#         )\
#         .whenNotMatchedInsertAll(
#             condition="new_df.date > current_date() - INTERVAL 7 days")\
#         .execute()

def main():
  checkpoint_path = 's3://bucket/checkpoint/'
  streaming_writer = (
      spark.readStream
      .format('kafka')
      .options(**KAFKA_CONFIG)
      .load()
      .writeStream
      .trigger("30 minute")
      .foreachBatch(foreach_batch_cdc_func)
      .option('checkpointLocation', checkpoint_path)
      .start()
      .awaitTermination()
  )

if __name__ == '__main__':
    main()
