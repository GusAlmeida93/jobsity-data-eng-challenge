# import libraries
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf

warehouse_location = abspath('spark-warehouse')

if __name__ == '__main__':

    # init session
    spark = SparkSession \
            .builder \
            .appName("etl-trips-py") \
            .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
            .enableHiveSupport() \
            .getOrCreate()

    print(SparkConf().getAll())

    spark.sparkContext.setLogLevel("INFO")

    get_trips_file = "gs://jobsity-processing-zone/files/trips/*.csv"
    
    df_trips = spark.read.format('csv')\
        .option('header', True)\
        .option('delimiter', ',')\
        .load(get_trips_file)

    df_trips.createOrReplaceTempView("trips")

    df_join = spark.sql("""
        SELECT *
        FROM trips
    """)

    df_join.write.format("parquet").mode("overwrite").save("gs://jobsity-curated-zone/ds_trips")

    spark.stop()