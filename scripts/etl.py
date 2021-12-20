from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, TimestampType, DecimalType
from pyspark.sql.window import Window
import configparser

def load_data(spark):

    print("Loading Data...")

    return spark.read \
        .format("json") \
        .options(header="True", multiline="True") \
        .load(link_file)

def cast_to_type(df):

    print("Casting columns to the appropriate types...")

    return df.withColumn("carpark_id", f.col("carpark_id").cast(IntegerType())) \
        .withColumn("floor", f.col("floor").cast(IntegerType())) \
        .withColumn("id", f.col("id").cast(IntegerType())) \
        .withColumn("number", f.col("number").cast(IntegerType())) \
        .withColumn("timestamp", f.col("timestamp").cast(TimestampType()))

def further_preprocessing(df, window):

    print("Creating important columns...")

    return df.withColumn("year", f.year(df.timestamp))\
        .withColumn("month", f.month(df.timestamp))\
        .withColumn("week_day", f.dayofweek(df.timestamp))\
        .withColumn("day_of_year", f.dayofyear(df.timestamp))\
        .withColumn("day", f.dayofmonth(df.timestamp))\
        .withColumn("hour", f.hour(df.timestamp))\
        .withColumn("min", f.minute(df.timestamp))\
        .withColumn("sec", f.second(df.timestamp))\
        .withColumn("week_no", f.weekofyear(df.timestamp)) \
        .withColumn("time_stop", f.col("timestamp").cast(DecimalType(15, 3))) \
        .withColumn("time_start", f.lag("time_stop",1).over(window)) \
        .withColumn("time_diff", (f.col("time_stop") - f.col("time_start")).cast(IntegerType())) \
        .withColumn("time_diff_lag", f.lag("time_diff", -1).over(window)) \
        .withColumn("day_diff", (f.col("time_diff")/86400).cast(IntegerType())) \
        .withColumn("hour_diff", f.floor((f.col("time_diff") - (f.col("day_diff")*86400))/3600).cast(IntegerType())) \
        .withColumn("minute_diff", f.floor((f.col("time_diff") - (f.col("day_diff")*86400) - (f.col("hour_diff")*3600))/60).cast(IntegerType())) \
        .withColumn("second_diff", f.round((f.col("time_diff") - (f.col("day_diff")*86400) - (f.col("hour_diff")*3600) - (f.col("minute_diff")*60)),3).cast(IntegerType())) \
        .withColumn("timestamp_diff", f.concat(f.col("day_diff"), f.lit(" "), f.col("hour_diff"), f.lit(":"), f.col("minute_diff"), f.lit(":"), f.col("second_diff"))) \
        .withColumn("time_stay", f.when(f.col("status") == "BUSY", f.col("time_diff")).otherwise(None)) \
        .withColumn("time_diff", f.when(f.col("status") == "NOT_CALIBRATED", f.lit(0)) \
        .otherwise(f.col("time_diff"))) \
        .withColumn("timestamp_diff", f.when(f.col("status") == "NOT_CALIBRATED", f.lit(0)) \
        .otherwise(f.col("timestamp_diff")))

def drop_short_stays(df, window):

    print("Dropping short stays...")

    return df.withColumn("lead_time_diff", f.lead("time_diff", 1).over(window)) \
        .filter((f.col("time_diff") > 60) & (f.col("status") == "BUSY")).filter(f.col("lead_time_diff") > 60)

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("InternalProject") \
        .master("local[*]") \
        .getOrCreate()

    config = configparser.ConfigParser()
    config.read("/home/project/resources/config.ini")

    container =  config.get("BLOB_CONFIGS", "container")
    storage_account =  config.get("BLOB_CONFIGS", "storage_account")
    query_string =  config.get("BLOB_CONFIGS", "query_string")

    spark.sparkContext.setLogLevel("Error")
    spark.conf.set(f"fs.azure.sas.{container}.{storage_account}.blob.core.windows.net", query_string)

    link_file = f"wasbs://{container}@{storage_account}.blob.core.windows.net/*.json"
    window = Window.partitionBy(f.col("id")).orderBy(f.col("timestamp"))

    df = drop_short_stays(further_preprocessing(cast_to_type(load_data(spark)), window), window)

    df.show(5, truncate=False)
    df.printSchema()

    spark.stop()