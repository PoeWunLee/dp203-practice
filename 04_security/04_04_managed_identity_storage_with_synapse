DROP EXTERNAL TABLE ActivityLog;
DROP EXTERNAL DATA SOURCE srcActivityLog;


--main focus here
--no credentials specified or required from user. When Managed Identity is created on Entra ID side, it allows e.g. synapse to authenticate with storage account.
CREATE DATABASE SCOPED CREDENTIAL storageManagedIdentity
WITH IDENTITY ='Managed Identity'  

CREATE EXTERNAL DATA SOURCE srcActivityLog
WITH 
(
    LOCATION='abfss://data@datalake4000700.dfs.core.windows.net',
    CREDENTIAL=storageManagedIdentity
)

CREATE EXTERNAL FILE FORMAT parquetFileFormat WITH
(
    FORMAT_TYPE=PARQUET,
    DATA_COMPRESSION='org.apache.hadoop.io.compress.SnappyCodec'
)


CREATE EXTERNAL TABLE ActivityLog
(
   [Correlationid] varchar(200),
   [Operationname] varchar(300),
   [Status] varchar(100),
   [Eventcategory] varchar(100),
   [Level] varchar(100),
   [Time] varchar(100),
   [Subscription] varchar(200),
   [Eventinitiatedby] varchar(1000),
   [Resourcetype] varchar(300),
   [Resourcegroup] varchar(1000),
   [Resource] varchar(2000))
WITH (
    LOCATION='/parquet/ActivityLog01.parquet',
    DATA_SOURCE=srcActivityLog,
    FILE_FORMAT=parquetFileFormat
)

