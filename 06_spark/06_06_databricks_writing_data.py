### -----Writing Data----- ##

#write data to databrick's side database


df.write.mode("overwrite").saveAsTable("ActivityLogdata")

%sql
SELECT * FROM ActivityLogdata


#write to ADL
filePath="abfss://data@datalake7000.dfs.core.windows.net/parquet/ActivityLog01.parquet"

spark.conf.set(
  "fs.azure.account.key.datalake7000.dfs.core.windows.net",
  "ES5EbbZX68ooZmT3vuvoJ31KR0bOfNiTo8DVA7F39vJSPpNADN0yGOqgn+vqATQdPQFSVXRlk6eT+AStcnsu6Q=="
)

val df = spark.write.format("parquet").save(filePath)


#writeStream to synapse
##requires checkpoint
checkpoint="/tmp/checkpoint"
schemalocation="/tmp/schema"

filtereddf.writeStream.format("com.databricks.spark.sqldw")\
.option("url", "jdbc:sqlserver://dataworkspace40040.sql.azuresynapse.net:1433;database=datapool") \
.option("user","sqladminuser")
.option("password","Microsoft@123")
.option("tempDir", "abfss://staging@datalake4000700.dfs.core.windows.net/databricks")
.option("forwardSparkAzureStorageCredentials", "true")
.option("dbTable", "dbo.PoolActivityLog")
.option("checkpointLocation", checkpoint).start()