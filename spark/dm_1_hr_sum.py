from pyspark.sql import functions as F
from pyspark.sql.functions import col,concat
from pyspark.sql import SparkSession,SQLContext
def main():
    spark = SparkSession.builder \
        .master('spark://localhost:7077') \
        .appName('DM_1_HR_SUM') \
        .config('spark.some.config.option', 'some-value') \
        .getOrCreate()

    sqlContext = SQLContext(spark.sparkContext)
    transformed_data = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="meter_customer_data",
                                                                                keyspace="tangez_transformed").load()

    aggregate_data = transformed_data.groupBy('ert_id').agg(F.sum(col('consumption_data'))).withColumnRenamed('sum(consumption_data)','consumption_data_sum')

    aggregate_data.write.format('org.apache.spark.sql.cassandra').mode('append') \
        .options(table='consumption_1_hour_sum', keyspace='tangez_data_marts').save()

if __name__ == '__main__':
    main()