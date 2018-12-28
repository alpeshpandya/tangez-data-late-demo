import time
import datetime
from pyspark.sql.functions import lit,unix_timestamp,col,concat
from pyspark.sql import SparkSession
def main():
    spark = SparkSession.builder \
        .master('spark://localhost:7077') \
        .appName('INGEST_METER_DATA') \
        .config('spark.some.config.option', 'some-value') \
        .getOrCreate()

    interval_1_ts = datetime.datetime.fromtimestamp(time.time()).replace(hour=11, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S')
    interval_1_df = spark.read.option('header', True).option('inferSchema', True).csv(
        'file:///home/guest/data/meter_data_1.csv')
    interval_1_df = interval_1_df.withColumn('received_ts',
                                             unix_timestamp(lit(interval_1_ts), 'yyyy-MM-dd HH:mm:ss').cast(
                                                 'timestamp')).withColumn('meter_data_id',concat(col('ert_id_ms_bits'),col('_c0'))).drop('_c0')
    interval_1_df.write.format('org.apache.spark.sql.cassandra').mode('append') \
        .options(table='meter_data', keyspace='tangez_staging').save()

    interval_2_ts = datetime.datetime.fromtimestamp(time.time()).replace(hour=11, minute=15, second=0).strftime(
        '%Y-%m-%d %H:%M:%S')
    interval_2_df = spark.read.option('header', True).option('inferSchema', True).csv(
        'file:///home/guest/data/meter_data_2.csv')
    interval_2_df = interval_2_df.withColumn('received_ts',
                                             unix_timestamp(lit(interval_2_ts), 'yyyy-MM-dd HH:mm:ss').cast(
                                                 'timestamp')).withColumn('meter_data_id',concat(col('ert_id_ms_bits'),col('_c0'))).drop('_c0')

    interval_2_df.write.format('org.apache.spark.sql.cassandra').mode('append') \
        .options(table='meter_data', keyspace='tangez_staging').save()

    interval_3_ts = datetime.datetime.fromtimestamp(time.time()).replace(hour=11, minute=30, second=0).strftime(
        '%Y-%m-%d %H:%M:%S')
    interval_3_df = spark.read.option('header', True).option('inferSchema', True).csv(
        'file:///home/guest/data/meter_data_4.csv')
    interval_3_df = interval_3_df.withColumn('received_ts',
                                             unix_timestamp(lit(interval_3_ts), 'yyyy-MM-dd HH:mm:ss').cast(
                                                 'timestamp')).withColumn('meter_data_id',concat(col('ert_id_ms_bits'),col('_c0'))).drop('_c0')

    interval_3_df.write.format('org.apache.spark.sql.cassandra').mode('append') \
        .options(table='meter_data', keyspace='tangez_staging').save()

    interval_4_ts = datetime.datetime.fromtimestamp(time.time()).replace(hour=11, minute=45, second=0).strftime(
        '%Y-%m-%d %H:%M:%S')
    interval_4_df = spark.read.option('header', True).option('inferSchema', True).csv(
        'file:///home/guest/data/meter_data_5.csv')
    interval_4_df = interval_4_df.withColumn('received_ts',
                                             unix_timestamp(lit(interval_4_ts), 'yyyy-MM-dd HH:mm:ss').cast(
                                                 'timestamp')).withColumn('meter_data_id',concat(col('ert_id_ms_bits'),col('_c0'))).drop('_c0')

    interval_4_df.write.format('org.apache.spark.sql.cassandra').mode('append') \
        .options(table='meter_data', keyspace='tangez_staging').save()


if __name__ == '__main__':
    main()