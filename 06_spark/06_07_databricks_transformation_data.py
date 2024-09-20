#filtering data

display(df.where(df['Resourcegroup']=="app-grp"))

from pyspark.sql.functions import col
filtereddf=df.filter(col('Resourcegroup').isNotNull())
display(filtereddf)

#group bys

summarydf=filtereddf.groupby('Resourcegroup').count()
display(summarydf)

#some date functions
from pyspark.sql.functions import year,month,dayofyear
display(df.select(year(col("Time")).alias("Year"),month(col("Time")).alias("Month"),dayofyear(col("Time")).alias("Day of year")))

from pyspark.sql.functions import to_date
display(df.select(to_date(col("Time"),"dd-mm-yyyy").alias("Date")))

#JSON - array of objects, use explode. If need to access objects attributes, just use . notation
from pyspark.sql.functions import explode
explodeddf=df.select(col("customerid"),col("customername"),explode(col("courses")))
display(explodeddf)

from pyspark.sql.functions import explode
explodeddf=df.select(col("customerid"),col("customername"),explode(col("courses")),col("details.city"),col("details.mobile"))
display(explodeddf)

##Adding an alias for the column name in the output
from pyspark.sql.functions import explode
explodeddf=df.select(col("customerid"),col("customername"),explode(col("courses")).alias("coursename"),col("details.city"),col("details.mobile"))
display(explodeddf)

#going back to historical snapshot of data. by default retention 30 days
%sql
DESCRIBE HISTORY 

'''
example output
+-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+-----------------+-------------+--------------------+
|version|          timestamp|userId|userName|operation| operationParameters| job|notebook|clusterId|readVersion|   isolationLevel|isBlindAppend|    operationMetrics|
+-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+-----------------+-------------+--------------------+
|      5|2019-07-29 14:07:47|   ###|     ###|   DELETE|[predicate -> ["(...|null|     ###|      ###|          4|WriteSerializable|        false|[numTotalRows -> ...|
|      4|2019-07-29 14:07:41|   ###|     ###|   UPDATE|[predicate -> (id...|null|     ###|      ###|          3|WriteSerializable|        false|[numTotalRows -> ...|
|      3|2019-07-29 14:07:29|   ###|     ###|   DELETE|[predicate -> ["(...|null|     ###|      ###|          2|WriteSerializable|        false|[numTotalRows -> ...|
|      2|2019-07-29 14:06:56|   ###|     ###|   UPDATE|[predicate -> (id...|null|     ###|      ###|          1|WriteSerializable|        false|[numTotalRows -> ...|
|      1|2019-07-29 14:04:31|   ###|     ###|   DELETE|[predicate -> ["(...|null|     ###|      ###|          0|WriteSerializable|        false|[numTotalRows -> ...|
|      0|2019-07-29 14:01:40|   ###|     ###|    WRITE|[mode -> ErrorIfE...|null|     ###|      ###|       null|WriteSerializable|         true|[numFiles -> 2, n...|
+-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+-----------------+-------------+--------------------+


'''

%sql
SELECT COUNT(*) FROM appdb.activitylogdata VERSION AS OF 2
SELECT COUNT(*) FROM appdb.activitylogdata TIMESTAMP AS OF 2





