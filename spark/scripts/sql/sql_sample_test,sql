-- ==========================================================
-- Sample SQL Queries
-- Ecommerce Batch Pipeline
-- ==========================================================

-- 1. Total number of events
SELECT COUNT(*) AS total_events
FROM customer_events;


-- 2. Top 10 brands by number of events
SELECT
    brand,
    COUNT(*) AS total_events
FROM customer_events
GROUP BY brand
ORDER BY total_events DESC
LIMIT 10;


-- 3. Average product price by brand
SELECT
    brand,
    ROUND(AVG(price), 2) AS average_price
FROM customer_events
WHERE brand IS NOT NULL
GROUP BY brand
ORDER BY average_price DESC;


-- 4. Number of events by event type
SELECT
    event_type,
    COUNT(*) AS total_events
FROM customer_events
GROUP BY event_type
ORDER BY total_events DESC;


-- 5. Top 10 most active users
SELECT
    user_id,
    COUNT(*) AS total_events
FROM customer_events
GROUP BY user_id
ORDER BY total_events DESC
LIMIT 10;


-- 6. Top 10 highest priced products
SELECT
    product_id,
    brand,
    price
FROM customer_events
ORDER BY price DESC
LIMIT 10;