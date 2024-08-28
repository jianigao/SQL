SELECT
    DISTINCT customer_id
FROM 
    customer_data.customer_address
--DISTINCT让结果只出现一次


SELECT
    LENGTH(country) AS letters_in_country
FROM 
    customer_data.customer_address


SELECT
    country
FROM 
    customer_data.customer_address
WHERE
    LENGTH(country) > 2


SELECT
    DISTINCT customer_id
FROM 
    customer_data.customer_address
WHERE
    SUBSTR(country,1,2) = 'US'
--SUBSTR(value,starting point,number of characters)
--这里只提取前2位字符


SELECT
    state
FROM 
    customer_data.customer_address
WHERE
    LENGTH(state) > 2


SELECT
    DISTINCT customer_id
FROM 
    customer_data.customer_address
WHERE
    TRIM(state) = 'OH'


SELECT
    purchase_price
FROM 
    customer_data.customer_purchase
ORDER BY
    purchase_price DESC


SELECT
    CAST(purchase_price AS FLOAT64)
FROM 
    customer_data.customer_purchase
ORDER BY
    CAST(purchase_price AS FLOAT64) DESC
--CAST change data types


SELECT
    date,
    purchase_price
FROM 
    customer_data.customer_purchase
WHERE
    date BETWEEN '2020-12-01' AND '2020-12-31'


SELECT
    CAST(date AS date) AS date_only,
    purchase_price
FROM 
    customer_data.customer_purchase
WHERE
    date BETWEEN '2020-12-01' AND '2020-12-31'


SELECT
    CONCAT(product_code, product_color) AS new_product_code
FROM 
    customer_data.customer_purchase
WHERE
    product = 'couch'


SELECT
    COALESCE(product, product_code) AS product_info
FROM 
    customer_data.customer_purchase
--COALESCE uses the second value if the first is null


SELECT
    customer_id,
    CASE
        WHEN first_name = 'Tnoy' THEN 'Tony'
        WHEN first_name = 'Tmo' THEN 'Tom'
        WHEN first_name = 'Rachle' THEN 'Rachel'
        ELSE first_name
        END AS cleaned_name
FROM 
    customer_data.customer_name


SELECT
    *
FROM 
    movie_Data.movies
WHERE
    Genre = 'Comedy'
--WHERE acts like a filter


SELECT *
FROM `movie_Data.movies`
ORDER BY Release_Date
--ORDER BY acts like sorting
--Ascending order


SELECT *
FROM `movie_Data.movies`
ORDER BY Release_Date DESC


SELECT *
FROM `movie_Data.movies`
WHERE Genre = 'Comedy'
ORDER BY Release_Date DESC


SELECT *
FROM `movie_Data.movies`
WHERE Genre = 'Comedy'
AND Revenue > 300000000
ORDER BY Release_Date DESC


SELECT
    usertype,
    CONCAT(start_station_name," to ",end_station_name) AS route,
    COUNT(*) AS num_trips,
    ROUND(AVG(CAST(tripduration AS INT64)/60),2) AS duration
FROM 
    `bigquery-public-data.new_york.citibike_trips`
GROUP BY
    start_station_name, end_station_name, usertype
ORDER BY
    num_trips DESC
LIMIT 10
--In ROUND, /60 gives value in minutes, 2: 2 decimals
--LIMIT 10 shows only top 10 results


SELECT
    employees.name AS employee_name,
    employees.role AS employee_role,
    departements.name AS department_name
FROM
    employees
INNER JOIN
    departements ON 
    employees.departement_id = departements.departement_id


SELECT
    employees.name AS employee_name,
    employees.role AS employee_role,
    departements.name AS department_name
FROM
    employees
LEFT JOIN
    departements ON 
    employees.departement_id = departements.departement_id


SELECT
    employees.name AS employee_name,
    employees.role AS employee_role,
    departements.name AS department_name
FROM
    employees
RIGHT JOIN
    departements ON 
    employees.departement_id = departements.departement_id


SELECT
    employees.name AS employee_name,
    employees.role AS employee_role,
    departements.name AS department_name
FROM
    employees
FULL OUTER JOIN
    departements ON 
    employees.departement_id = departements.departement_id


SELECT
    orders.*,
    warehouse.warehouse_alias,
    warehouse.state
FROM
    warehouse_orders.Orders orders
JOIN
    warehouse_orders.Warehouse warehouse ON orders.warehouse_id = warehouse.warehouse_id
--create aliases for tables
--JOIN = INNER JOIN


SELECT
    COUNT(warehouse.state) AS num_states
FROM
    warehouse_orders.Orders orders
JOIN
    warehouse_orders.Warehouse warehouse ON orders.warehouse_id = warehouse.warehouse_id


SELECT
    COUNT(DISTINCT warehouse.state) AS num_states
FROM
    warehouse_orders.Orders orders
JOIN
    warehouse_orders.Warehouse warehouse ON orders.warehouse_id = warehouse.warehouse_id


SELECT
    warehouse.state AS state,
    COUNT(DISTINCT order_id) AS num_orders
FROM
    warehouse_orders.Orders orders
JOIN
    warehouse_orders.Warehouse warehouse ON orders.warehouse_id = warehouse.warehouse_id
GROUP BY
    warehouse.state


SELECT
    station_id,
    num_bikes_available,
    (SELECT
        AVG(num_bikes_available)
    FROM bigquery-public-data.new_york.citibike_stations) AS avg_num_bikes_available
FROM bigquery-public-data.new_york.citibike_stations


SELECT
    station_id,
    name,
    number_of_rides AS number_of_rides_starting_at_station
FROM
    (
        SELECT
            start_station_id,
            COUNT(*) number_of_rides
        FROM
            bigquery-public-data.new_york.citibike_trips
        GROUP BY
            start_station_id
    )
    AS station_num_trips
INNER JOIN 
    bigquery-public-data.new_york.citibike_stations ON station_id = start_station_id
ORDER BY
    number_of_rides DESC


SELECT
    station_id,
    name
FROM
    bigquery-public-data.new_york.citibike_stations
WHERE
    station_id IN
    (
        SELECT
            start_station_id
        FROM
            bigquery-public-data.new_york.citibike_trips
        WHERE
            usertype = 'Subscriber'
    )


SELECT
    Warehouse.warehouse_id,
    CONCAT(Warehouse.state,': ',Warehouse.warehouse_alias) AS warehouse_name,
    COUNT(Orders.order_id) AS number_of_orders,
    (SELECT
        COUNT(*)
    FROM warehouse_orders.Orders Orders)
    AS total_orders,
    CASE
        WHEN COUNT(Orders.order_id)/(SELECT COUNT(*) FROM warehouse_orders.Orders Orders) <= 0.20
        THEN "Fulfilled 0-20% of Orders"
        WHEN COUNT(Orders.order_id)/(SELECT COUNT(*) FROM warehouse_orders.Orders Orders) > 0.20
        AND COUNT(Orders.order_id)/(SELECT COUNT(*) FROM warehouse_orders.Orders Orders) <= 0.60
        THEN "Fulfilled 21-60% of Orders"
        ELSE "Fulfilled more than 60% of Orders"
    END AS fulfillment_summary
FROM warehouse_orders.Warehouse Warehouse
LEFT JOIN warehouse_orders.Orders Orders
    ON Orders.warehouse_id = Warehouse.warehouse_id
GROUP BY
    Warehouse.warehouse_id,
    warehouse_name
HAVING
    COUNT(Orders.order_id) > 0
--WHERE cannot be used with aggregate functions (+ - * /); it can be used before GROUP BY
--If we use GROUP BY first, then we use HAVING function as a filter


SELECT
    Date,
    Small_Bags,
    Large_Bags,
    XLarge_Bags,
    Total_Bags,
    Small_Bags + Large_Bags + XLarge_Bags AS Total_Bags_Calc
FROM avocado_data.avocado_prices


SELECT *
FROM 
    avocado_data.avocado_prices
WHERE
    Total_Bags != Total_Bags_Calc


SELECT
    Date,
    Region,
    Total_Bags,
    Small_Bags,
    (Small_Bags / Total_Bags)*100 AS Small_Bags_Percent
FROM 
    avocado_data.avocado_prices
WHERE 
    Total_Bags != 0
--Total_Bags should not be 0


SELECT
    EXTRACT(YEAR FROM STARTTIME) AS year,
    COUNT(*) AS number_of_rides
FROM
    bigquery-public-data.new_work.citibike_trips
GROUP BY
    year
ORDER BY
    year DESC
--GROUP BY: a command that groups rows that have the same values from a table into summary rows


--The WITH clause is a type of temporary table that you can query from multiple times
WITH trips_over_1_hr AS (
    SELECT *
    FROM
        bigquery-public-data.new_york.citibike_trips
    WHERE
        tripduration >= 60
    )
--Count how many trips are 60+ minutes long
SELECT
    COUNT(*) AS cnt
FROM
    trips_over_1_hr


--SELECT INTO copies data from one table into a new table but it doesn't add the new table to the database
SELECT
    *
INTO
    AfricaSales
FROM
    GlobalSales
WHERE
    Region = "Africa"


--CREATE TABLE adds the table into the database
CREATE TABLE AfricaSales AS
(
    SELECT *
    FROM GlobalSales
    WHERE Region = "Africa"
)

