-- show databases;
CREATE DATABASE SQL_WITH_BARA;
CREATE DATABASE SQL_WITH_DWA;

USE SQL_WITH_BARA ;

-- Window Function
SELECT ProductID,SUM(Sales) OVER(PARTITION BY ProductID) AS TOTALSalesByProducts
FROM orders;

-- Window Function
SELECT OrderID,OrderDate,ProductID,SUM(Sales) OVER(PARTITION BY ProductID) AS TOTALSalesByProducts
FROM orders;


SELECT *
FROM Orders;

-- Find the total sales across all orders
-- Aditinally Provide deatils such order id,order date

SELECT OrderID,ORDERDate,SUM(Sales) OVER() 
FROM Orders ;

-- Find the Total sales for each product
SELECT ProductID,OrderID,OrderDate ,SUM(sales) OVER (PARTITION BY ProductID) TotalSales
FROM Orders;

-- Find the Total sales across all orders AND
-- Find the Total sales for each product
SELECT ProductID,OrderID,OrderDate ,SUM(sales) OVER () AS TotalSales ,
SUM(sales) OVER (PARTITION BY ProductID) AS TotalSalesBYProducts
FROM Orders;


-- Find the total sales for each combination of product and order status
SELECT ProductID,OrderID,OrderDate , OrderStatus,sales,SUM(sales) OVER () AS TotalSales ,
SUM(sales) OVER (PARTITION BY ProductID) AS TotalSalesBYProducts,
SUM(sales) OVER (PARTITION BY ProductID,orderstatus) AS salesBYProductandSales
FROM Orders;


-- Rank each order based on their sales FROM highest to lowest
-- aditionally provode deatils such order id and orderdate

SELECT OrderID,OrderDate,sales,RANK() OVER (ORDER BY Sales DESC) as RANKSALES
FROM Orders; 



-- Find the total number of orders
SELECT COUNT(*) AS TotalOrders
FROM Orders;

-- Aditionally provide details such orderId,order date
SELECT OrderID,OrderDate,COUNT(*) OVER() TotalOrders
FROM Orders;

SELECT OrderID,OrderDate,CustomerID,COUNT(*) OVER() TotalOrders,
COUNT(*) OVER(PARTITION BY CustomerID) AS OrderByCustomers
FROM Orders;
