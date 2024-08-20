--fact table
SELECT
hd.[SalesOrderID], hd.[CustomerID], hd.[OrderDate], hd.[SubTotal], hd.[TaxAmt], hd.[Freight], hd.[TotalDue],
dt.[OrderQty], dt.[ProductID], dt.[UnitPrice], dt.[UnitPriceDiscount], dt.[LineTotal]
FROM [SalesLT].[SalesOrderHeader] hd INNER JOIN [SalesLT].[SalesOrderDetail] dt
on hd.[SalesOrderID]=dt.[SalesOrderID];


--dim table
SELECT pd.[ProductID], pd.[ProductNumber], pd.[Color], pd.[ProductCategoryID], ct.[Name] as 'ProductCategoryName'
FROM [SalesLT].[Product] pd INNER JOIN [SalesLT].[ProductCategory] ct
ON pd.[ProductCategoryID] = ct.[ProductCategoryID];