---------
CREATE OR REPLACE FUNCTION get_hotel_reservations_by_city(city VARCHAR(100))
RETURNS TABLE(
  hotel_name VARCHAR(100),
  date DATE,
  nights INTEGER,
  price DECIMAL(10, 2),
  room_type VARCHAR(50)
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    h.name AS hotel_name,
    hr.date,
    hr.nights,
    hr.prices,
    r.room_type
  FROM
    hotel_reservations hr
    INNER JOIN rooms r ON hr.room_id = r.room_id
    INNER JOIN hotels h ON r.hotel_id = h.hotel_id
  WHERE
    h.city = get_hotel_reservations_by_city.city;
END;
$$ LANGUAGE plpgsql;
----------

SELECT * FROM get_hotel_reservations_by_city('Perry');


----------
SELECT c.name AS company_name,
  r.date,
  ROUND(r.prices / r.duration, 2) AS price_per_day,
  c.city
FROM rent_reservations r
  INNER JOIN cars car ON r.car_id = car.car_id
  INNER JOIN car_rental_companies c ON car.car_company_id = c.car_company_id
ORDER BY c.city, c.name, r.date;
----------


---------- Calculate the average rating and total revenue for each hotel, and order by descending total revenue:
WITH hotel_avg_rating AS (
  SELECT
    hotel_id,
    AVG(rating_score) AS avg_rating
  FROM
    hotel_reviews
  GROUP BY
    hotel_id
),
hotel_revenue AS (
  SELECT
    hotels.hotel_id,
    SUM(hotel_reservations.prices) AS total_revenue
  FROM
    hotels
    JOIN rooms ON hotels.hotel_id = rooms.hotel_id
    JOIN hotel_reservations ON rooms.room_id = hotel_reservations.room_id
  GROUP BY
    hotels.hotel_id
),
hotel_rating_revenue AS (
  SELECT
    hotel_avg_rating.hotel_id,
    hotels.name,
    avg_rating,
    COALESCE(total_revenue, 0) AS total_revenue
  FROM
    hotel_avg_rating
    JOIN hotels ON hotel_avg_rating.hotel_id = hotels.hotel_id
    LEFT JOIN hotel_revenue ON hotel_avg_rating.hotel_id = hotel_revenue.hotel_id
)
SELECT
  hotel_id,
  name,
  avg_rating,
  total_revenue
FROM
  hotel_rating_revenue
ORDER BY
  total_revenue DESC;
----------


---------- Calculates the average rental review ratings for each car manufacturer and model
WITH rental_reviews_stats AS (
  SELECT
    cars.manufacturer,
    cars.model,
    AVG(rental_reviews.rating_score) AS avg_rating
  FROM
    rental_reviews
    JOIN cars ON rental_reviews.car_id = cars.car_id
  GROUP BY
    cars.manufacturer,
    cars.model
),
rental_stats AS (
  SELECT
    cars.manufacturer,
    cars.model,
    COUNT(rent_reservations.id) AS total_rentals
  FROM
    rent_reservations
    JOIN cars ON rent_reservations.car_id = cars.car_id
  GROUP BY
    cars.manufacturer,
    cars.model
),
combined_stats AS (
  SELECT
    COALESCE(r.manufacturer, s.manufacturer) AS manufacturer,
    COALESCE(r.model, s.model) AS model,
    r.avg_rating,
    s.total_rentals
  FROM
    rental_reviews_stats r
    FULL OUTER JOIN rental_stats s ON (r.manufacturer = s.manufacturer) AND (r.model = s.model)
)
SELECT
  manufacturer,
  model,
  COALESCE(avg_rating, 0) AS avg_rating
FROM
  combined_stats
ORDER BY
  avg_rating DESC, total_rentals DESC;
----------

---------- Distribution of ticket classes purchased by guests based on their loyalty group
SELECT
  user_loyalty.loyalty_group,
  ticket_prices.classes,
  COUNT(ticket_prices.ticket_id) AS total_tickets,
  COUNT(ticket_prices.ticket_id)::FLOAT / SUM(COUNT(ticket_prices.ticket_id)) OVER (PARTITION BY user_loyalty.loyalty_group) * 100 AS percentage
FROM
  ticket_prices
  JOIN flights ON ticket_prices.flight_id = flights.flight_id
  JOIN guests ON flights.passenger_id = guests.guest_id
  JOIN user_loyalty ON guests.guest_id = user_loyalty.guest_id
GROUP BY
  user_loyalty.loyalty_group,
  ticket_prices.classes
ORDER BY
  user_loyalty.loyalty_group,
  ticket_prices.classes;
----------


---------- Calculate the total revenue and total loyalty points earned for each user.
CREATE OR REPLACE FUNCTION user_revenue_and_loyalty()
RETURNS TABLE (
  guest_id INT,
  guest_name VARCHAR,
  total_revenue DECIMAL,
  total_loyalty_points DECIMAL
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    g.guest_id,
    (g.first_name || ' ' || g.last_name)::VARCHAR as guest_name,
    SUM(hr.prices) + SUM(tp.prices) + SUM(rr.prices) as total_revenue,
    SUM(ul.activity) as total_loyalty_points
  FROM
    guests g
  LEFT JOIN hotel_reservations hr ON g.guest_id = hr.guest_id
  LEFT JOIN ticket_prices tp ON g.guest_id = tp.reservation_id
  LEFT JOIN rent_reservations rr ON g.guest_id = rr.guest_id
  LEFT JOIN user_loyalty ul ON g.guest_id = ul.guest_id
  GROUP BY
    g.guest_id,
    guest_name
  ORDER BY
    total_revenue DESC;
END;
$$ LANGUAGE plpgsql;
-------------

SELECT * FROM user_revenue_and_loyalty();



------------- Calculate the total revenue generated by each loyalty group:
WITH guest_revenue AS (
  SELECT
    guest_id,
    SUM(prices) AS total_revenue
  FROM
    hotel_reservations
  GROUP BY
    guest_id
),
loyalty_group_revenue AS (
  SELECT
    user_loyalty.loyalty_group,
    SUM(guest_revenue.total_revenue) AS total_revenue
  FROM
    user_loyalty
    JOIN guest_revenue ON user_loyalty.guest_id = guest_revenue.guest_id
  GROUP BY
    user_loyalty.loyalty_group
)
SELECT
  loyalty_group,
  total_revenue
FROM
  loyalty_group_revenue
ORDER BY
  total_revenue DESC;
-------------



------------- Cohort analysis: Calculate the retention rate of guests based on their first reservation month
WITH cohort AS (
  SELECT
    guest_id,
    DATE_TRUNC('month', MIN(date)) AS cohort_month
  FROM
    hotel_reservations
  GROUP BY
    guest_id
),
monthly_activity AS (
  SELECT
    cohort.guest_id,
    DATE_TRUNC('month', hotel_reservations.date) AS activity_month
  FROM
    cohort
    JOIN hotel_reservations ON cohort.guest_id = hotel_reservations.guest_id
),
cohort_size AS (
  SELECT
    cohort_month,
    COUNT(DISTINCT guest_id) AS cohort_size
  FROM
    cohort
  GROUP BY
    cohort_month
),
activity_by_cohort AS (
  SELECT
    cohort.cohort_month,
    monthly_activity.activity_month,
    COUNT(DISTINCT cohort.guest_id) AS active_guests
  FROM
    cohort
    JOIN monthly_activity ON cohort.guest_id = monthly_activity.guest_id
  GROUP BY
    cohort.cohort_month,
    monthly_activity.activity_month
),
retention_rate AS (
  SELECT
    activity_by_cohort.cohort_month,
    activity_by_cohort.activity_month,
    active_guests::FLOAT / cohort_size::FLOAT * 100 AS retention_rate
  FROM
    activity_by_cohort
    JOIN cohort_size ON activity_by_cohort.cohort_month = cohort_size.cohort_month
)
SELECT
  cohort_month,
  activity_month,
  retention_rate
FROM
  retention_rate
ORDER BY
  cohort_month,
  activity_month;
----------



---------- RFM analysis: Calculate Recency, Frequency, and Monetary value for each guest
WITH guest_activity AS (
  SELECT
    guest_id,
    MAX(date) AS last_reservation_date,
    COUNT(*) AS total_reservations,
    SUM(prices) AS total_revenue
  FROM
    hotel_reservations
  GROUP BY
    guest_id
),
rfm AS (
  SELECT
    guest_id,
    EXTRACT(EPOCH FROM (NOW() - last_reservation_date)) / 86400 AS recency,
    total_reservations AS frequency,
    total_revenue AS monetary
  FROM
    guest_activity
)
SELECT
  guest_id,
  recency,
  frequency,
  monetary
FROM
  rfm
ORDER BY
  guest_id;
----------


---------- Compare the percentage of guests participated in 1, 2, 3 activities
WITH guest_activity_count AS (
  SELECT
    guests.guest_id,
    COUNT(DISTINCT hotel_reservations.id) AS hotel_count,
    COUNT(DISTINCT rent_reservations.id) AS rent_count,
    COUNT(DISTINCT flights.flight_id) AS flight_count
  FROM
    guests
    LEFT JOIN hotel_reservations ON guests.guest_id = hotel_reservations.guest_id
    LEFT JOIN rent_reservations ON guests.guest_id = rent_reservations.guest_id
    LEFT JOIN flights ON guests.guest_id = flights.passenger_id
  GROUP BY
    guests.guest_id
),
activity_stats AS (
  SELECT
    'Hotel Only' AS activity_type,
    SUM(CASE WHEN hotel_count > 0 AND rent_count = 0 AND flight_count = 0 THEN 1 ELSE 0 END)::FLOAT / COUNT(*)::FLOAT * 100 AS percentage
  FROM
    guest_activity_count
  UNION ALL
  SELECT
    'Rent Only',
    SUM(CASE WHEN hotel_count = 0 AND rent_count > 0 AND flight_count = 0 THEN 1 ELSE 0 END)::FLOAT / COUNT(*)::FLOAT * 100
  FROM
    guest_activity_count
  UNION ALL
  SELECT
    'Flight Only',
    SUM(CASE WHEN hotel_count = 0 AND rent_count = 0 AND flight_count > 0 THEN 1 ELSE 0 END)::FLOAT / COUNT(*)::FLOAT * 100
  FROM
    guest_activity_count
  UNION ALL
  SELECT
    'Hotel & Rent',
    SUM(CASE WHEN hotel_count > 0 AND rent_count > 0 AND flight_count = 0 THEN 1 ELSE 0 END)::FLOAT / COUNT(*)::FLOAT * 100
  FROM
    guest_activity_count
  UNION ALL
  SELECT
    'Hotel & Flight',
    SUM(CASE WHEN hotel_count > 0 AND rent_count = 0 AND flight_count > 0 THEN 1 ELSE 0 END)::FLOAT / COUNT(*)::FLOAT * 100
  FROM
    guest_activity_count
  UNION ALL
  SELECT
    'Rent & Flight',
    SUM(CASE WHEN hotel_count = 0 AND rent_count > 0 AND flight_count > 0 THEN 1 ELSE 0 END)::FLOAT / COUNT(*)::FLOAT * 100
  FROM
    guest_activity_count
  UNION ALL
  SELECT
    'All Activities',
    SUM(CASE WHEN hotel_count > 0 AND rent_count > 0 AND flight_count > 0 THEN 1 ELSE 0 END)::FLOAT / COUNT(*)::FLOAT * 100
  FROM
    guest_activity_count
)
SELECT
  activity_type,
  percentage
FROM
  activity_stats
ORDER BY
  activity_type;
----------


----------Find cheapest of date/city
CREATE OR REPLACE FUNCTION get_cheapest_hotel_reservation(p_date DATE, p_city VARCHAR)
RETURNS TABLE (
  reservation_id INT,
  hotel_name VARCHAR,
  city VARCHAR,
  reservation_date DATE,
  nights INT,
  prices DECIMAL(10,2),
  room_type VARCHAR
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    hr.id AS reservation_id,
    h.name AS hotel_name,
    h.city,
    hr.date,
    hr.nights,
    hr.prices,
    r.room_type
  FROM
    hotel_reservations hr
    INNER JOIN rooms r ON hr.room_id = r.room_id
    INNER JOIN hotels h ON r.hotel_id = h.hotel_id
  WHERE
    hr.date = p_date AND h.city = p_city
  ORDER BY
    hr.prices
  LIMIT 1;
END;
$$ LANGUAGE plpgsql;
----------


SELECT * FROM get_cheapest_hotel_reservation('2021-5-10','Paola');




