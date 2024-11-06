-- The queries executed in Pig are the same two simple done in Hive.
-- These were also done in Pig to verify the results

-- 1. Total Sales By Product Line
-- The first query calculates total sales by each product line, giving an overview of which product categories contribute the most to revenue.
-- This query groups the data by ProductLine and sums up the Total sales in each category, then orders the results in descending order by TotalSales and limits the output to the top 5 product lines.

grouped_data = GROUP sales_data BY ProductLine;
product_line_sales = FOREACH grouped_data GENERATE group AS ProductLine,
        SUM(sales_data.Total) AS TotalSales;
sorted_sales = ORDER product_line_sales BY TotalSales DESC;
top_5_sales = LIMIT sorted_sales 5;
-- DUMP top_5_sales;

-- 2. Average Rating By City for Orders Above 500
-- The second query finds the average customer rating by city for orders over 500.
-- This filters records to include only orders with Total greater than 500, groups them by City, calculates the average Rating, and sorts the results by the highest AverageRating values.

high_value_orders = FILTER sales_data BY Total > 500;
grouped_by_city = GROUP high_value_orders BY City;
average_rating_by_city = FOREACH grouped_by_city GENERATE group AS City,
        AVG(high_value_orders.Rating) AS AverageRating;
sorted_ratings = ORDER average_rating_by_city BY AverageRating DESC;
-- DUMP sorted_ratings;