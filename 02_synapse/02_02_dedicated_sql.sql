--loading external table data into dedicated sql pool table
--initialising script in 2a

--[POLYBASE]--
--- note that no additional SAS token required 
---as polybase will use underlying credentials made on activityLog
CREATE TABLE PoolActivityLog
WITH (
    DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT * FROM activityLog;


select * from PoolActivityLog;

--[COPY INTO command]--

drop table PoolActivityLog;
---step 1: to have table created first
CREATE TABLE PoolActivityLog(
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
    DISTRIBUTION=ROUND_ROBIN
);

---step 2: Copy into, with credentials required to specify
------CSV------
COPY INTO PoolActivityLog
FROM 'https://storageforexampoe.blob.core.windows.net/data/ActivityLog-01.csv'
WITH(
    FILE_TYPE='CSV',
    FIRST_ROW=2,
    CREDENTIALS=(
        IDENTITY='Shared Access Signature',
        SECRET='sv=2022-11-02&ss=bf&srt=sco&sp=rlx&se=2025-08-06T18:27:47Z&st=2024-08-06T10:27:47Z&spr=https&sig=cKnovoTT3l68sDsTHiHmp7znOO4o6PNOSz0oUE4oaf8%3D'
    )
);

------Parquet------
COPY INTO PoolActivityLog
FROM 'https://storageforexampoe.blob.core.windows.net/data/ActivityLog-01.parquet'
WITH(
    FILE_TYPE='PARQUET',
    CREDENTIAL=(
        IDENTITY='Shared Access Signature',
        SECRET='sv=2022-11-02&ss=bf&srt=sco&sp=rlx&se=2025-08-06T18:27:47Z&st=2024-08-06T10:27:47Z&spr=https&sig=cKnovoTT3l68sDsTHiHmp7znOO4o6PNOSz0oUE4oaf8%3D'
    )
);

select * from PoolActivityLog;

--[PIPELINES]--
DELETE from PoolActivityLog;
--see integrate tab


CREATE TABLE dimProduct(
    ProductSK int IDENTITY(1,1) NOT NULL, --SURROGATE KEY: start from number 1, increment of 1
    ProductID int NOT NULL, --BUSINESS KEY
    ProductNum varchar(100) NOT NULL,
    Color varchar(20) NOT NULL,
    ProductCategoryID int NOT NULL,
    ProductCategoryName varchar(200) NOT NULL
)
WITH(
    DISTRIBUTION=REPLICATE
)