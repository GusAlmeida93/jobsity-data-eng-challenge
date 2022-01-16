# import libraries
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F

warehouse_location = abspath('spark-warehouse')

if __name__ == '__main__':

    # init session
    spark = SparkSession \
            .builder \
            .appName("etl-trips-py") \
            .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
            .enableHiveSupport() \
            .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    get_trips_file = "gs://jobsity-processing-zone/files/trips/*.csv"
    destination_path = "gs://jobsity-curated-zone/ds_trips"
    
    df = spark.read.format('csv')\
        .option('header', True)\
        .option('delimiter', ',')\
        .load(get_trips_file)
        

    df = df.withColumn("timestamp",F.to_timestamp("datetime"))

    df = df.dropDuplicates(subset=['origin_coord','destination_coord', 'datetime'])

    df = df.withColumn('week_year', F.weekofyear(df['timestamp']))
    
    df = df.withColumn('year', F.year(df['timestamp']))
    
    df_avg_region = df.groupBy('region', 'year','week_year').agg(F.count('region').alias('count_region'))
    
    df.write.format("parquet").mode("overwrite").save(f'{destination_path}/trips')
    df_avg_region.write.format("parquet").mode("overwrite").save(f'{destination_path}/avg_trips_region')

    spark.stop()