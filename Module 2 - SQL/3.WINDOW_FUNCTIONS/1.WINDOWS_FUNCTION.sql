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

-- Rank the orders based on their sales from highest to lowest
SELECT OrderID,ProductID,Sales,
ROW_NUMBER() OVER(ORDER BY sales DESC),
RANK() OVER(ORDER BY sales DESC),
DENSE_RANK() OVER(ORDER BY sales DESC)
FROM Orders;

-- Find the top higesht sales each product
SELECT OrderID,ProductID,Sales,
ROW_NUMBER() OVER(partition by ProductID ORDER BY sales DESC) as RankByProducts
FROM Orders;

-- Rank top one in each product
SELECT * 
FROM (
SELECT OrderID,ProductID,Sales,
ROW_NUMBER() OVER(partition by ProductID ORDER BY sales DESC) as RankByProducts
FROM Orders 
) as t WHERE t.RankByProducts = 1;


-- Find the lowest 2 Customers based on the total sales 
SELECT * 
FROM (
SELECT CustomerID,SUM(Sales) AS TotalSales,
ROW_NUMBER() OVER(ORDER BY SUM(Sales)) as RankofCustomers
FROM Orders 
GROUP BY CustomerID
) as t WHERE t.RankofCustomers <= 2;


-- WINDOW LAG and LEAD Function
-- Time series analysis
-- Month over Month analysis
-- Analyze the month-over-month perfomrance by finding the percentage change
-- in sales between the current and previous months

SELECT 
    MONTH(OrderDate) AS OrderMonth,
    SUM(Sales) AS CurrentMonthSales,
    LAG(SUM(Sales)) OVER(ORDER BY MONTH(OrderDate)) AS PreviousMonthSales
FROM Orders
GROUP BY MONTH(OrderDate);


SELECT *,
       CurrentMonthSales - PreviousMonthSales AS MoM_change
FROM (
    SELECT 
        OrderMonth,
        CurrentMonthSales,
        LAG(CurrentMonthSales) OVER (ORDER BY OrderMonth) AS PreviousMonthSales
    FROM (
        SELECT 
            MONTH(OrderDate) AS OrderMonth,
            SUM(Sales) AS CurrentMonthSales
        FROM Orders
        GROUP BY MONTH(OrderDate)
    ) m
) t;


-- Find the lowest and highest sales for each product
SELECT OrderID,ProductID,Sales,
FIRST_VALUE(sales) OVER (PARTITION BY ProductID ORDER BY sales ) Lowestsales,
LAST_VALUE(sales) OVER (PARTITION BY ProductID ORDER BY sales
ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) HighestSales,
FIRST_VALUE(Sales) OVER (PARTITION BY ProductID ORDER BY Sales DESC ) HighestSales2,
MIN(sales) OVER (PARTITION BY ProductID) LowestSales2,
MAX(sales) OVER (PARTITION BY ProductID) LowestSales3
FROM Orders;

