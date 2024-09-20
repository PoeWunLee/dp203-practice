df = spark\
.read
.option("header","true") \
.csv("abfss://csv@datalake244434.dfs.core.windows.net/Log.csv") \
display(df)
 
# If you want to use SQL statements against your data frame, you can create a view on top of the data frame
 
df.createOrReplaceTempView("Logdata")
 
%%sql
SELECT * FROM Logdata
 
# You can also use PySpark as it is to look at the data
# Basically use the SQL API to work with your data
 
sqldf=spark.sql("SELECT * FROM Logdata")
display(sqldf)
 
# To write to pooldb
# We need to match the schema properly
# Make sure the Azure admin has the storage blob reader and contributor role
 
from pyspark.sql.types import StructType,StringType,TimestampType
 
dataSchema = StructType() \
    .add("Correlation id", StringType(), True) \
    .add("Operation name", StringType(), True) \
    .add("Status", StringType(), True) \
    .add("Event category",StringType(), True) \
    .add("Level",StringType(),True) \
    .add("Time", TimestampType(), True) \
    .add("Subscription",StringType(), True) \
    .add("Event initiated by", StringType(), True) \
    .add("Resource type",StringType(),True) \
    .add("Resource group",StringType(),True) \
    .add("Resource",StringType(),True)
 
df = spark.read.format("csv") \
.option("header",True) \
.schema(dataSchema) \
.load("abfss://csv@datalake244434.dfs.core.windows.net/Log.csv")
 
display(df)
 
 
 
import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
 
df.write \
    .option(Constants.SERVER,"dataworkspace2000939.sql.azuresynapse.net") \
    .option(Constants.USER,"sqladminuser") \
    #.option(Constants.PASSWORD,"") \ commenting to avoid git push error on secrets
    .option(Constants.DATA_SOURCE,"pooldb") \
    .option(Constants.TEMP_FOLDER,"abfss://staging@datalake244434.dfs.core.windows.net") \
   # .option(Constants.STAGING_STORAGE_ACCOUNT_KEY,"") \ commenting to avoid git push error on secrets
    .mode("overwrite") \
    .synapsesql("pooldb.dbo.logdata") 