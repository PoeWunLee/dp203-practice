df = spark.read.csv('abfss://data@datalake7000.dfs.core.windows.net/csv/ActivityLog01.csv')
display(df)

df = spark.read.option("header", True).csv('abfss://data@datalake7000.dfs.core.windows.net/csv/ActivityLog01.csv')
display(df)

df = spark.read.load("abfss://data@datalake7000.dfs.core.windows.net/parquet/ActivityLog01.parquet"
      , format="parquet")
display(df)