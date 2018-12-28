from pyspark.sql import SparkSession
def main():
    spark = SparkSession.builder \
        .master('spark://localhost:7077') \
        .appName('INGEST_CUSTOMER_DATA') \
        .config('spark.some.config.option', 'some-value') \
        .getOrCreate()

    customer_data = spark.read.option("header", True).option("inferSchema", True).csv(
        'file:///home/guest/data/customer_data.csv')
    cleaned_customer_data = customer_data.drop('_c0').withColumnRenamed('st_address', 'address')

    cleaned_customer_data.write.format("org.apache.spark.sql.cassandra").mode('append') \
        .options(table="customer_data", keyspace="tangez_staging").save()


if __name__ == '__main__':
    main()