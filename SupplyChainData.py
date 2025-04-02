import pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("ReadExcel").getOrCreate()

# Read Excel File
pdf = ps.read_excel("/content/Pan Pacific -Input Data.xlsx")
df = spark.createDataFrame(pdf)

# Replace NaN Values
df = df.replace("NaN", None)

# Create Customer DataFrame
customer_df = df.filter(F.col("Type")=="Bill To").select(
    F.lit("12345").alias("id"),
    F.col("Name").alias("name"),
    F.col("Code").alias("account_number"),
    F.lit("{xyz.com}").alias("domain_name"),
    F.lit("xyz").alias("tenant_id")
)

# Create Invoice DataFrame
invoice_df = df.join(customer_df.drop('name'), F.col('Code') == F.col('account_number'), 'inner').withColumn("Type", F.regexp_replace(F.col("Type"), "Bill", "Ship"))

# Create Ship To DataFrame
shipto_df = df.join(customer_df.drop('name'), F.col('Type') == F.lit('Ship To'), 'inner')

# Create Bill To DataFrame
billto_df = df.join(customer_df.drop('name'), F.col('Code') == F.col('account_number'), 'inner').withColumn("Code", F.lit(None))

# Combine DataFrames
outputData_Union_df = invoice_df.union(shipto_df)
outputData_Union_df = outputData_Union_df.union(billto_df)

# Mapping to Standard Layout
outputData_df = outputData_Union_df.withColumn('InputId', F.row_number().over(Window.orderBy(F.col("Code").asc_nulls_last()))) \
    .withColumn('country', F.when(F.col('Zip').contains('-'), "US").otherwise("USA")) \
    .withColumn('type', F.lower(F.regexp_replace(F.concat(F.lit("customer_"), F.col("Type")), ' ', '')))

outputData_df = outputData_df.select(
    F.col("InputId").alias("id"),
    F.col("Name").alias("name"),
    F.col("Address1").alias("address_1"),
    F.col("Address2").alias("address_2"),
    F.lit(None).alias("address_3"),
    F.col("City").alias("city"),
    F.col("State").alias("state"),
    "country",
    F.col("Zip").alias("zip"),
    F.col("tenant_id").alias("tenant_id"),
    F.lit("FALSE").alias("deleted"),
    F.col("Type").alias("type"),
    F.col("Code").alias("shipto_no"),
    F.col("id").alias("customer_id"),
)

# Convert to Pandas DataFrames
outputData_pd = outputData_df.toPandas()
customer_pd = customer_df.toPandas()

# Write to Excel
excel_path = "panpacific_outputData.xlsx"
with ps.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
    outputData_pd.to_excel(writer, sheet_name="outputData", index=False)
    customer_pd.to_excel(writer, sheet_name="customer", index=False)
	
