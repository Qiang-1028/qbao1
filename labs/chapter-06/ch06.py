# ***************************************

# In Python Page 228 of E-book
# ********
# @author Qiang Bao
# ********

from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date
from pyspark.sql.functions import split

spark = SparkSession.builder.appName("A20453021 - lab chapter 06").getOrCreate()


# Save this as a .csv uncompressed to the location: .save("hdfs://namenode/user/controller/output/your-hawk-id-here/00/save-file-action") (00 is whichever dataset you were assigned to)

df2 = spark.read.text("hdfs://namenode/user/controller/ncdc/raw/60/60.txt")

df2.withColumn('WeatherStation', df2['value'].substr(5, 6)) \
    .withColumn('WBAN', df2['value'].substr(11, 5)) \
    .withColumn('ObservationDate',to_date(df2['value'].substr(16,8), 'yyyyMMdd')) \
    .withColumn('ObservationHour', df2['value'].substr(24, 4).cast(IntegerType())) \
    .withColumn('Latitude', df2['value'].substr(29, 6).cast('float') / 1000) \
    .withColumn('Longitude', df2['value'].substr(35, 7).cast('float') / 1000) \
    .withColumn('Elevation', df2['value'].substr(47, 5).cast(IntegerType())) \
    .withColumn('WindDirection', df2['value'].substr(61, 3).cast(IntegerType())) \
    .withColumn('WDQualityCode', df2['value'].substr(64, 1).cast(IntegerType())) \
    .withColumn('SkyCeilingHeight', df2['value'].substr(71, 5).cast(IntegerType())) \
    .withColumn('SCQualityCode', df2['value'].substr(76, 1).cast(IntegerType())) \
    .withColumn('VisibilityDistance', df2['value'].substr(79, 6).cast(IntegerType())) \
    .withColumn('VDQualityCode', df2['value'].substr(86, 1).cast(IntegerType())) \
    .withColumn('AirTemperature', df2['value'].substr(88, 5).cast('float') /10) \
    .withColumn('ATQualityCode', df2['value'].substr(93, 1).cast(IntegerType())) \
    .withColumn('DewPoint', df2['value'].substr(94, 5).cast('float')) \
    .withColumn('DPQualityCode', df2['value'].substr(99, 1).cast(IntegerType())) \
    .withColumn('AtmosphericPressure', df2['value'].substr(100, 5).cast('float')/ 10) \
    .withColumn('APQualityCode', df2['value'].substr(105, 1).cast(IntegerType())) 
    .drop('value').write.format("csv").mode("overwrite").option("header","true").save("hdfs://namenode/user/controller/output/A20453021/60/")

# Open the newly saved CSV file for reading again and print the schema and .count() values

df3 = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load("hdfs://namenode/user/controller/output/A20453021/60")


count_df3 = df3.count()

print("**** Qiang Bao CSV schema: ****")
df3.printSchema()
print("**** Qiang Bao CSV count: ****")
print(count_df3)

# Save the dataframe as a csv file compressed with snappy

df2.write.format("csv").mode("overwrite").option("header","true").option("compression","snappy").save("hdfs://namenode/user/controller/output/A20453021/60_snappy")

# Open the newly saved compressed csv snappy file and print the schema and .count() values

df5 = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load("hdfs://namenode/user/controller/output/A20453021/60_snappy")

count_df5 = df5.count()

print("**** Qiang Bao CSV schema: ****")
df5.printSchema()
print("**** Qiang Bao CSV count: ****")
print(count_df5)

# Save the dataframe as a JSON uncompressed file

df2.write.format("json").mode("overwrite").option("header","true").save("hdfs://namenode/user/controller/output/A20453021/60_json/")

# Open the newly saved JSON file for reading again and print the schema and .count() values

df6 = spark.read.format("json") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load("hdfs://namenode/user/controller/output/A20453021/60_json")

count_df6 = df6.count()
print("**** Qiang Bao JSON schema: ****")
df6.printSchema()
print("**** Qiang Bao JSON count: ****")
print(count_df6)

# Save the dataframe as a JSON file compressed with snappy

df2.write.format("json").mode("overwrite").option("header","true").option("compression","snappy").save("hdfs://namenode/user/controller/output/A20453021/60_json_snappy/")

# Open the newly saved compressed JSON file and print the schema and .count() values

df7 = spark.read.format("json") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load("hdfs://namenode/user/controller/output/A20453021/60_json_snappy")

count_df7 = df7.count()
print("**** Qiang Bao JSON snappy schema: ****")
df7.printSchema()
print("**** Qiang Bao JSON snappy count: ****")
print(count_df7)


# Save the dataframe as an XML file

df2.write.format("xml").mode("overwrite").option("header","true").save("hdfs://namenode/user/controller/output/A20453021/60_xml/")

# Open the newly saved XML file for reading again and print the schema and .count() values

df9 = spark.read.format("com.databricks.spark.xml") \
        .option("header", "true") \
        .option("rowTag", "Entry") \
        .load("hdfs://namenode/user/controller/output/A20453021/60_xml/")

count_df9 = df9.count()
print("**** Qiang Bao XML schema: ****")
df9.printSchema()
print("**** Qiang Bao XML count: ****")
print(count_df9)



# Save the dataframe as a Parquet file

df2.write.format("parquet").mode("overwrite").save("hdfs://namenode/user/controller/output/A20453021/60_parquet/")


# Open the newly saved Parquet file for reading, create three additional columns splitting the ObservationDate column into, year, month, date columns.

df_p = spark.read.format("parquet")\
        .load("hdfs://namenode/user/controller/output/A20453021/60_parquet/")

newdate = split(df_p["ObservationDate"],'-')

df_p2 = df_p.withColumn('year',newdate.getItem(0).cast(IntegerType()))\
        .withColumn('month',newdate.getItem(1).cast(IntegerType()))\
        .withColumn('date',newdate.getItem(2).cast(IntegerType()))

print("**** Qiang Bao parquet schema(with new column!): ****")
df_p2.printSchema()

spark.stop