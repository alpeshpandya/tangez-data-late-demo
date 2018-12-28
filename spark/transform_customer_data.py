from pyspark.sql.functions import col,concat
from pyspark.sql import SparkSession,SQLContext
def main():
    spark = SparkSession.builder \
        .master('spark://localhost:7077') \
        .appName('TRANSFORM_CUSTOMER_DATA') \
        .config('spark.some.config.option', 'some-value') \
        .getOrCreate()

    sqlContext = SQLContext(spark.sparkContext)
    customer_data = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="customer_data",
                                                                                keyspace="tangez_staging").load()

    meter_data = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="meter_data",
                                                                                keyspace="tangez_transformed").load()

    joined_data = meter_data.join(customer_data,'ert_id')

    joined_data.write.format('org.apache.spark.sql.cassandra').mode('append') \
        .options(table='meter_customer_data', keyspace='tangez_transformed').save()

    customer_data.write.format('org.apache.spark.sql.cassandra').mode('append') \
        .options(table='customer_data', keyspace='tangez_transformed').save()


if __name__ == '__main__':
    main()