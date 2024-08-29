/*

Scenario: Assume we have Fact table FactInternetSales and we partition by OrderDateKey

Now is a new year (2001) where we want to create partition for the previous year (2000-01-01 to 2000-12-31), so that query performance can still be top notch for end users

*/


--table DDL FactInternetSales we are working with
CREATE TABLE [dbo].[FactInternetSales]
(
        [ProductKey]            int          NOT NULL
    ,   [OrderDateKey]          int          NOT NULL
    ,   [CustomerKey]           int          NOT NULL
    ,   [PromotionKey]          int          NOT NULL
    ,   [SalesOrderNumber]      nvarchar(20) NOT NULL
    ,   [OrderQuantity]         smallint     NOT NULL
    ,   [UnitPrice]             money        NOT NULL
    ,   [SalesAmount]           money        NOT NULL
)
WITH
(   CLUSTERED COLUMNSTORE INDEX
,   DISTRIBUTION = HASH([ProductKey])
,   PARTITION   (   [OrderDateKey] RANGE RIGHT FOR VALUES
                    (20000101
                    )
                )
);

INSERT INTO dbo.FactInternetSales
VALUES (1,19990101,1,1,1,1,1,1);

INSERT INTO dbo.FactInternetSales
VALUES (1,20000101,1,1,1,1,1,1);


/* Recall during partition switching
1. Source and target table must have the same partitions 
2. Split range (i.e. creating a new partition) requires the partition to be empty
*/


--Step 1: Create a new temporary table with CTAS for partition 2, with same partitions, distribution and indexing
CREATE TABLE dbo.FactInternetSales_20000101
    WITH    (   DISTRIBUTION = HASH(ProductKey)
            ,   CLUSTERED COLUMNSTORE INDEX              
            ,   PARTITION   (   [OrderDateKey] RANGE RIGHT FOR VALUES
                                (20000101
                                )
                            )
)
AS
SELECT *
FROM    FactInternetSales
WHERE   1=2; --this ensures empty table for convenience

--Step 2: Perform partition switching
ALTER TABLE FactInternetSales SWITCH PARTITION 2 TO FactInternetSales_20000101 PARTITION 2;


--Step 3: Now that main table is empty for year 2000, split the range to include new partition for 2021
ALTER TABLE FactInternetSales SPLIT RANGE (20010101);

--Step 4: Aligning new partition boundaries with another CTAS on the temp table
CREATE TABLE [dbo].[FactInternetSales_20000101_20010101]
    WITH    (   DISTRIBUTION = HASH([ProductKey])
            ,   CLUSTERED COLUMNSTORE INDEX
            ,   PARTITION   (   [OrderDateKey] RANGE RIGHT FOR VALUES
                                (20000101,20010101
                                )
                            )
            )
AS
SELECT  *
FROM    [dbo].[FactInternetSales_20000101]
WHERE   [OrderDateKey] >= 20000101
AND     [OrderDateKey] <  20010101;


--Step 5: Lastly re-insert into main fact table the 2000 data with another parition switch
ALTER TABLE dbo.FactInternetSales_20000101_20010101 SWITCH PARTITION 2 TO dbo.FactInternetSales PARTITION 2;


--Alternatively steps 1 to 2 can be simplified now with TRUNCATE_TARGET=ON, 
--with the following CTAS
CREATE TABLE [dbo].[FactInternetSales_NewSales]
    WITH    (   DISTRIBUTION = HASH([ProductKey])
            ,   CLUSTERED COLUMNSTORE INDEX
            ,   PARTITION   (   [OrderDateKey] RANGE RIGHT FOR VALUES
                                (20000101,20010101
                                )
                            )
            )
AS
SELECT  *
FROM    [dbo].[FactInternetSales]
WHERE   [OrderDateKey] >= 20000101
AND     [OrderDateKey] <  20010101
;

INSERT INTO dbo.FactInternetSales_NewSales
VALUES (1,20000101,2,2,2,2,2,2);

ALTER TABLE dbo.FactInternetSales_NewSales SWITCH PARTITION 2 TO dbo.FactInternetSales PARTITION 2 WITH (TRUNCATE_TARGET = ON);
