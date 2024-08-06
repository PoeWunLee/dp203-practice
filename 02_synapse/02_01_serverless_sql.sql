--initalise new DB. after this refresh and select appdb as the database for the following
CREATE DATABASE [appdb];

--[Configure data source]--
---encryption key for secrets
CREATE MASTER KEY ENCRYPTION BY PASSWORD='P@ssw@rd123';

---scoped credential - this is where SAS token will sit
CREATE DATABASE SCOPED CREDENTIAL storageAccessCredSAS
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2022-11-02&ss=bf&srt=sco&sp=rlx&se=2025-08-06T18:27:47Z&st=2024-08-06T10:27:47Z&spr=https&sig=cKnovoTT3l68sDsTHiHmp7znOO4o6PNOSz0oUE4oaf8%3D';

---finally create the external data source with scoped credential referencing above created
CREATE EXTERNAL DATA SOURCE srcActivity
WITH (
    LOCATION='https://storageforexampoe.blob.core.windows.net/data',
    CREDENTIAL=storageAccessCredSAS

);

--[Configure file format]--

---e.g.1 Parquet
CREATE EXTERNAL FILE FORMAT parquetFileFormat
WITH(
    FORMAT_TYPE= PARQUET,
    DATA_COMPRESSION='org.apache.hadoop.io.compress.SnappyCodec'
);
---e.g.2 csv
CREATE EXTERNAL FILE FORMAT csvFileFormat
WITH(
    FORMAT_TYPE= DELIMITEDTEXT,
    FORMAT_OPTIONS(
        FIELD_TERMINATOR=',',
        FIRST_ROW = 2
    )
);
---e.g.3 JSON
CREATE EXTERNAL FILE FORMAT jsonFileFormat
WITH(
    FORMAT_TYPE= JSON,
    DATA_COMPRESSION='org.apache.hadoop.io.compress.SnappyCodec'
);

--[configure external table def]
CREATE EXTERNAL TABLE activityLog(
    [Correlationid] varchar(200),
    [Operationname]	varchar(300),
    [Status]	varchar(100),
    [Eventcategory]	varchar(100),
    [Level]	varchar(100),
    [Time]	varchar(100),
    [Subscription]	varchar(200),
    [Eventinitiatedby]	varchar(1000),
    [Resourcetype]	varchar(300),
    [Resourcegroup]	varchar(1000),
    [Resource] varchar(2000)
)
WITH(
    LOCATION='/*.parquet',
    DATA_SOURCE=srcActivity,
    FILE_FORMAT=parquetFileFormat
);



--JSON use OPENROWSET
--prerequisite : current login user requires Storage Blob Data Read access
select 
    JSON_VALUE(jsonContent, '$.Correlationid') as [Correlationid],
    JSON_VALUE(jsonContent, '$.Operationname') as [Operationname],
    JSON_VALUE(jsonContent, '$.Eventcategory') as [Eventcategory],
    JSON_VALUE(jsonContent, '$.Level') as [Level],
    JSON_VALUE(jsonContent, '$.Time') as [Time],
    JSON_VALUE(jsonContent, '$.Subscription') as [Subscription],
    JSON_VALUE(jsonContent, '$.Eventinitiatedby') as [Eventinitiatedby],
    JSON_VALUE(jsonContent, '$.Resourcetype') as [Resourcetype],
    JSON_VALUE(jsonContent, '$.Resourcegroup') as [Resourcegroup],
    JSON_VALUE(jsonContent, '$.Resource') as [Resource]

from OPENROWSET(
    bulk 'https://storageforexampoe.blob.core.windows.net/data/ActivityLog-01.json',
    format = 'csv',
    fieldterminator ='0x0b',
    fieldquote = '0x0b',
    rowterminator='0x0a'
)with (jsonContent varchar(MAX) ) as rows


--Test on quering external data

select count(*) from activityLog;

--
