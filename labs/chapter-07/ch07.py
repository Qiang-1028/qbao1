'''
Lab-Chapter-07

 @author Qiang Bao
'''
'''
Execute instructions: spark-submit --packages com.databricks:spark-xml_2.12:0.12.0 ./ch07.py
'''
from pyspark.sql import SparkSession
import os

from pyspark.sql.types import (StructType, StructField, FloatType,
                               IntegerType,DateType,
                               StringType)

current_dir = os.path.dirname(__file__)
relative_path_csv = "./sample-code/1920-ncdc.csv/ncdc-1920.csv"
relative_path_csv_snappy = "./sample-code/1920-ncdc-snappy.csv/ncdc-1920-snappy.csv"
relative_path_json_singleline = "./sample-code/1920-json-singleline.json/ncdc-1920.json"
relative_path_json_multiline = "./sample-code/data/20m.json"
relative_path_json_bad = "./sample-code/1920-json-singleline.json/ncdc-bad-1920.json"
relative_path_xml =  "./sample-code/1920-xml/ncdc-1920.xml"
relative_path_parquet = "./sample-code/1920-ncdc.parquet/ncdc-1920-snappy.parquet"

absolute_file_path_csv = os.path.join(current_dir, relative_path_csv)
absolute_file_path_csv_snappy = os.path.join(current_dir, relative_path_csv_snappy)
absolute_file_path_json_singleline = os.path.join(current_dir, relative_path_json_singleline)
absolute_file_path_json_multiline = os.path.join(current_dir, relative_path_json_multiline)
absolute_file_path_json_bad = os.path.join(current_dir, relative_path_json_bad)
absolute_file_path_xml = os.path.join(current_dir, relative_path_xml)
absolute_file_path_parquet = os.path.join(current_dir, relative_path_parquet)


# Creates a session on a local master
spark = SparkSession.builder.appName("Read CSV and show schema") \
    .master("local[*]").getOrCreate()

print("Using Apache Spark v{}".format(spark.version))

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe

df_csv = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load(absolute_file_path_csv)
count_csv = df_csv.count()





df_csv_snappy = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load(absolute_file_path_csv_snappy)



df_json_singleline = spark.read.format("json") \
        .option("header", "true") \
        .option("multiline", False) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load(absolute_file_path_json_singleline)
count_json_singleline = df_json_singleline.count()



df_json_multiline = spark.read.format("json") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load(absolute_file_path_json_multiline)
count_json_multiline = df_json_multiline.count()


df_json_bad = spark.read.format("json") \
        .option("header", "true") \
        .option("multiline", False) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("mode", "DROPMALFORMED") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load(absolute_file_path_json_bad)
count_json_bad = df_json_bad.count()


df_xml = spark.read.format("com.databricks.spark.xml") \
        .option("header", "true") \
        .option("rowTag", "Entry") \
        .load(absolute_file_path_xml)
count_xml = df_xml.count()

df_parquet = spark.read.format("parquet") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("mode", "DROPMALFORMED") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load(absolute_file_path_parquet)
count_parquet = df_parquet.count()





schema = StructType([StructField('WeatherStation',StringType(),False),
                     StructField('WBAN',StringType(),True),
                     StructField('ObservationDate',StringType(),False),
                     StructField('ObservatioonHous',IntegerType(),True),
                     StructField('Latitude',StringType(),True),
                     StructField('Longitude',StringType(),True),
                     StructField('Elevation',IntegerType(),True),
                     StructField('WinDirection',IntegerType(),True),
                     StructField('WDQualityCode',IntegerType(),True),
                     StructField('SkyCeilingHeight',IntegerType(),True),
                     StructField('SCQualityCode',IntegerType(),True),
                     StructField('VisibilityDistance',IntegerType(),True),
                     StructField('VDQualityCode',IntegerType(),True),
                     StructField('AirTemperature',FloatType(),True),
                     StructField('ATQualityCode',IntegerType(),True),
                     StructField('DewPoint',FloatType(),True),
                     StructField('DPQualityCode',IntegerType(),True),
                     StructField('AtmosphericPressure',FloatType(),True),
                     StructField('APQualityCode',IntegerType(),True)])


df_csv2 = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .schema(schema) \
        .load(absolute_file_path_csv)
count_csv = df_csv2.count()

#  save as parquet
df_csv2.write.format("parquet").mode("overwrite").option("header","true").save("./new_files/CsvToParquet")
#  read new parquet
df_csv3 = spark.read.format("parquet") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("inferSchema", True) \
        .load("./new_files/CsvToParquet")


# Use the same dataframe from last job
# Save to JSON file (name: new-1920-ncdc.json)
df_csv2.write.format("json").mode("overwrite").option("header","true").save("./new_files/new-1920-ncdc.json")
df_csv2.write.format("json").mode("overwrite").option("header","true").option("compression","snappy").save("./new_files/new-1920-ncdc-snappy.json")




# Read CSV file save as XML with compression
df_csvtoxmlcp = spark.read.format("csv") \
    .option("header", "true") \
    .option("multiline", True) \
    .option("inferSchema", True) \
    .load(absolute_file_path_csv)

df_csvtoxmlcp.write.format("xml").mode("overwrite").option("header","true").option("compression","snappy").save("./new_files/XMLwithCp")


# Read XML file

df_readXml = spark.read.format("com.databricks.spark.xml")\
    .option("rootTag","ROWS")\
    .option("rowTag","Entry")\
    .load(absolute_file_path_xml)








print("**** Qiang Bao, Dataframe's schema: ****")
df_csv.printSchema()
print("**** Qiang Bao, count: ****")
print(count_csv)
print()



count_csv_snappy = df_csv_snappy.count()
print("**** Qiang Bao, snappy csv Dataframe's schema: ****")
df_csv_snappy.printSchema()
print("**** Qiang Bao, count: ****")
print(count_csv_snappy)




print("**** Qiang Bao, json singleline schema: ****")
df_json_singleline.printSchema()
print("**** Qiang Bao, count: ****")
print(count_json_singleline)
print()


print("**** Qiang Bao, json multiline schema: ****")
df_json_multiline.printSchema()
print("**** Qiang Bao, count: ****")
print(count_json_multiline)
print()

print("**** Qiang Bao, json bad schema: ****")
df_json_bad.printSchema()
print("**** Qiang Bao, count: ****")
print(count_json_bad)
print()

print("**** Qiang Bao, xml schema: ****")
df_xml.printSchema()
print("**** Qiang Bao, count: ****")
print(count_xml)
print()

print("**** Qiang Bao, parquet schema: ****")
df_parquet.printSchema()
print("**** Qiang Bao, count: ****")
print(count_parquet)
print()

print("**** Qiang Bao, CSV schema: ****")
df_csv2.printSchema()
print("**** Qiang Bao, CSV count: ****")
print(count_csv)
print("**** Qiang Bao, parquet schema:  ****")
df_csv3.printSchema()


# Read the same CSV as the last job and print schema
print("**** Qiang Bao, Read CSV and print schema****")
df_csv2.printSchema()


# Show number of records
print("**** Qiang Bao CSV to XML(cp) count: ****")
count_csvtoxmlcp = df_csvtoxmlcp.count()
print(count_csvtoxmlcp)
print("**** Qiang Bao CSV to XML(cp) schema: ****")
df_csvtoxmlcp.printSchema()


# show schema
print("**** Qiang Bao, Read XML show shcema:  ****")
df_readXml.printSchema()
# save as parquet
df_readXml.write.format("parquet").mode("overwrite").option("header","true").save("./new_files/ReadXmlSaveAsParquet")






spark.stop