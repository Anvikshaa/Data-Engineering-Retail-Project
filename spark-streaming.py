
# Import Required Lib and Dependencies 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

spark = SparkSession  \
        .builder  \
        .appName("RetailByAnvi14")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read raw_input from kafka topic shared via Upgrad 
df = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
        .option("subscribe","real-time-project")  \
        .option("auto.offset.reset","earliest")   \
        .load()

# Define Schema to get the raw data in the required way
Schema = StructType() \
        .add("invoice_no", LongType()) \
	    .add("country",StringType()) \
        .add("timestamp", TimestampType()) \
        .add("type", StringType()) \
        .add("total_items",IntegerType())\
        .add("isOrder",IntegerType()) \
        .add("isReturn",IntegerType()) \
        .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType()) 
        ])))


raw_input = df.select(from_json(col("value").cast("string"), Schema).alias("data")).select("data.*")


# Build Utility functions for calculated attributes

def isAnOrder(type):
   if type=="ORDER":
       return 1
   else:
       return 0

def isAReturn(type):
   if type=="RETURN":
       return 1
   else:
       return 0
       
def total_items(items):
   total_count = 0
   for item in items:
       total_count = total_count + item['quantity']
   return total_count

def total_items_cost(items,type):
   total_price = 0
   for item in items:
       total_price = total_price + item['unit_price'] * item['quantity']
   if type=="RETURN":
       return total_price * (-1)
   else:
       return total_price



# Define the UDFs along with the utility functions
isOrder = udf(isAnOrder, IntegerType())
isReturn = udf(isAReturn, IntegerType())
total_item_count = udf(total_items, IntegerType())
total_cost = udf(total_items_cost, FloatType())


# Console Output
input = raw_input \
       .withColumn("total_items", total_item_count(raw_input.items)) \
       .withColumn("total_cost", total_cost(raw_input.items,raw_input.type)) \
       .withColumn("isOrder", isOrder(raw_input.type)) \
       .withColumn("isReturn", isReturn(raw_input.type))


output = input \
       .select("invoice_no", "country", "timestamp","type","total_items","total_cost","isOrder","isReturn") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()

# Calculate Time based KPIs
agg_time = input \
    .withWatermark("timestamp","1 minutes") \
    .groupby(window("timestamp", "1 minutes", "1 minutes")) \
    .agg(count("invoice_no").alias("OPM"),sum("total_cost").alias("totalSalesVolume"),
        avg("total_cost").alias("averageTransactionSize"),
        avg("isReturn").alias("rateOfReturn")) \
    .select("window.start","window.end","totalSalesVolume","averageTransactionSize","rateOfReturn")
    
# Calculate Time and Country based KPIs
agg_time_country = input \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(window("timestamp", "1 minutes", "1 minutes"), "country") \
    .agg(count("invoice_no").alias("OPM"),sum("total_cost").alias("totalSalesVolume"),
        avg("isReturn").alias("rateOfReturn")) \
    .select("window.start","window.end","country", "OPM","totalSalesVolume","rateOfReturn")


# Write Time based KPI values
time = agg_time.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "TimeKPIs/") \
    .option("checkpointLocation", "TimeKPIs/cp/") \
    .trigger(processingTime="1 minutes") \
    .start()


# Write Time and country based KPI values
time_country = agg_time_country.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "TimeCountryKPIs/") \
    .option("checkpointLocation", "TimeCountryKPIs/cp/") \
    .trigger(processingTime="1 minutes") \
    .start()

time_country.awaitTermination()
