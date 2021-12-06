from pyspark.sql import SparkSession
import configparser

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

df = spark.read \
    .format("json") \
    .options(header="True", multiline="True") \
    .load(link_file)

df.show(truncate=False)
df.printSchema()

spark.stop()