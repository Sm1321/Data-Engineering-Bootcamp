/*Task: Find the Products that have a price higher than the 
average price of all the products */

SELECT *
FROM Orders;

SELECT *
FROM products;


-- Subquery
SELECT *
FROM(
	SELECT productID,product,Price, AVG(Price) OVER() AvgPrice
	FROM products
) as A WHERE Price > AvgPrice;


SELECT productID,product,Price, AVG(Price) OVER() AvgPrice
FROM products;

-- Rank the customer based on their total amount of sales
SELECT *,Rank() OVER(ORDER BY TotalSales DESC) as CustomerRank
FROM 
(SELECT CustomerID,SUM(sales) as TotalSales
From Orders
GROUP BY CustomerID) as T ;


-- Show the ProductID,Product name , prices and total number of order
-- Main Query
-- Only scaler result used in the SELECT Clause as a Subquery
SELECT ProductID,Product,Price,
    (SELECT COUNT(*) FROM Orders) AS TotalOrders
FROM Products;
