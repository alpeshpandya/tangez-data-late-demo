from pyspark.sql.functions import col,concat
from pyspark.sql import SparkSession,SQLContext
def main():
    spark = SparkSession.builder \
        .master('spark://localhost:7077') \
        .appName('TRANSFORM_METER_DATA') \
        .config('spark.some.config.option', 'some-value') \
        .getOrCreate()

    sqlContext = SQLContext(spark.sparkContext)
    stage_df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="meter_data",
                                                                                keyspace="tangez_staging").load()

    ert_combined_id_df = stage_df.withColumn('ert_id', concat(col('ert_id_ms_bits'), col('ert_id_ls_bits')))

    ert_combined_id_df.write.format('org.apache.spark.sql.cassandra').mode('append') \
        .options(table='meter_data', keyspace='tangez_transformed').save()


if __name__ == '__main__':
    main()