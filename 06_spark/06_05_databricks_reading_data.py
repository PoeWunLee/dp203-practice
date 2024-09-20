### -----Reading Data----- ##

#load data from filestore
filePath="/FileStore/parquet/ActivityLog01.parquet"

df = spark.read.load(filePath, format="parquet")
display(df)


#reading from ADL
%scala

val filePath="abfss://data@datalake7000.dfs.core.windows.net/parquet/ActivityLog01.parquet"

spark.conf.set(
  "fs.azure.account.key.datalake7000.dfs.core.windows.net",
  "" #account key
)

val df = spark.read.format("parquet").load(filePath)
                   
display(df)

#read streaming data from ADL - requires schemalocation
checkpoint="/tmp/checkpoint"
schemalocation="/tmp/schema"

rawdf = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schemalocation)
    .load(filePath)
)

display(rawdf)

#read from azure synapse - jdbc connector
logdf=spark.read.format("com.databricks.spark.sqldw")\
  .option("url","jdbc:sqlserver://dataworkspace40040.sql.azuresynapse.net;database=datapool") \
  .option("user","sqladminuser") \
  .option("password","Microsoft@123") \
  .option("tempDir", "abfss://staging@datalake4000700.dfs.core.windows.net/databricks") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("query","SELECT * FROM dbo.VehicleTollBooth").load()

