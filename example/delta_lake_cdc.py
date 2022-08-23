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
    'kafka.bootstrap.servers': config[self.env]['kafka_brokers'],
    'kafka.ssl.truststore.location': config['kafka']['truststore_path'],
    'kafka.security.protocol': 'SSL',
    'kafka.ssl.protocol': 'SSL',
    'assign': json.dumps(data_tags_partition_dict),
    'groupIdPrefix': config['streaming_setting']['group_id_prefix'],
    'minPartitions': config['streaming_setting']['min_partitions'],
    'maxOffsetsPerTrigger': config['streaming_setting']['qps_upperbound']*30*60,
    'startingOffsets': config['kafka']['offset_strategy'],
    'failOnDataLoss': True,
    'kafka.fetch.min.bytes': config['update_parameters_setting']['fetch_min_bytes'],
    'kafka.fetch.max.wait.ms': config['update_parameters_setting']['fetch_max_wait_ms']
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
    
## other transform situation ##
#     original_delta_df.alias('old_df')\
#         .merge(mirco_batch_df.alias('new_df'), "old_df.id = new_df.id")\
#         .whenMatchedDelete(condition = "new_df.op = 'd'")\
#         .whenMatchedUpdateAll(
#             condition = "new_df.op = 'u'"
#         )\
#         .whenNotMatchedInsertAll()\
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
