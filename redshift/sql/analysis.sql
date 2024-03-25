-- Number of Rides by Payment Method Type
SELECT pt.payment_type_id, pt.payment_type_name, COUNT(*)
FROM rides_fact rf
INNER JOIN payment_type_dim pt
ON rf.payment_type_id = pt.payment_type_id
GROUP BY pt.payment_type_id, pt.payment_type_name;

-- Number of Rides by Rate Code
SELECT rc.rate_code_id, rc.rate_code_name, COUNT(*)
FROM rides_fact rf
INNER JOIN rate_code_dim rc
ON rf.rate_code_id = rc.rate_code_id
GROUP BY rc.rate_code_id, rc.rate_code_name;

-- Distribution of Trip Distance -> trip distance in miles (Visualize the distribution of trip distances using a histogram 
-- or box plot to understand the typical length of rides.)
SELECT min(trip_distance), max(trip_distance), avg(trip_distance) FROM public.rides_fact;


-- Peak Hours for Rides: Analyze the distribution of pickup times to identify peak hours for rides during the day.
SELECT dd.hour AS hour, COUNT(*) AS ride_cnt
FROM rides_fact rf
INNER JOIN datetime_dim dd
ON rf.pickup_datetime_id = dd.datetime_id
GROUP BY dd.hour
ORDER BY COUNT(*) DESC;

-- Correlation between Fare Amount and Trip Distance: Investigate the relationship between fare amount and 
-- trip distance using a scatter plot to see if longer rides tend to have higher fares.
SELECT fare_amount, trip_distance
FROM rides_fact rf
WHERE trip_distance <> 0
ORDER BY fare_amount
;

-- Percentage of Rides with Airport Fee: Calculate the percentage of rides that incur an airport fee 
-- and visualize it using a pie chart or bar chart.
SELECT 
  SUM(CASE WHEN airport_fee > 0 THEN 1 ELSE 0 END) AS airport_fee_rides,
  SUM(CASE WHEN airport_fee = 0 THEN 1 ELSE 0 END) AS no_airport_fee_rides 
FROM rides_fact rf
;

-- Passenger Count Distribution: Explore the distribution of passenger counts to 
--  see how many rides typically have multiple passengers.
SELECT passenger_count, COUNT(*) AS rides_by_passenger_cnt
FROM rides_fact rf
GROUP BY passenger_count
;

-- Average Fare Amount per Passenger Count: Analyze how the average fare amount 
-- varies based on the number of passengers in a ride.
SELECT passenger_count, ROUND(AVG(fare_amount), 2) AS avg_fare_passenger_cnt
FROM rides_fact rf
WHERE fare_amount > 0
GROUP BY passenger_count
ORDER BY avg_fare_passenger_cnt DESC
;

-- Geospatial Analysis: Use the pickup and dropoff location IDs to perform geospatial analysis, 
-- such as visualizing the most common pickup and dropoff locations on a map.
SELECT ld.borough, COUNT(*) AS ride_cnt_by_borough
FROM location_dim ld
INNER JOIN rides_fact rf
ON ld.location_id = rf.pickup_location_id
GROUP BY ld.borough
;

-- Comparison of Fare Components: Compare the distribution of different fare components 
-- (e.g., tolls amount, tip amount) to understand their relative importance in the total fare.
WITH 
fare_stats AS (
  SELECT 
    SUM(total_amount) AS total_amount, 
    SUM(airport_fee) AS airport_fee, 
    SUM(congestion_surcharge) AS congestion_surcharge, 
    SUM(tolls_amount) AS tolls_amount, 
    SUM(improvement_surcharge) AS improvement_surcharge, 
    SUM(mta_tax) AS mta_tax, 
    SUM(extra) AS extra, 
    SUM(fare_amount) AS fare_amount, 
    SUM(tip_amount) AS tip_amount
  FROM rides_fact 
  WHERE fare_amount > 0 AND total_amount > 0
)

SELECT 
  ROUND((airport_fee / total_amount * 100), 2) AS airport_fee_dist,
  ROUND((congestion_surcharge / total_amount * 100), 2) AS congestion_surcharge_dist,
  ROUND((tolls_amount / total_amount * 100), 2) AS tolls_amount_dist,
  ROUND((improvement_surcharge / total_amount * 100), 2) AS improvement_surcharge_dist,
  ROUND((mta_tax / total_amount * 100), 2) AS mta_tax_dist,
  ROUND((extra / total_amount * 100), 2) AS extra_dist,
  ROUND((fare_amount / total_amount * 100), 2) AS fare_amount_dist,
  ROUND((tip_amount / total_amount * 100), 2) AS tip_amount_dist
FROM 
  fare_stats
;

-- Impact of Congestion Surcharge: Investigate the impact of the congestion surcharge on fare amounts 
-- by comparing rides before and after the introduction of the surcharge.
SELECT 
  dd.hour AS hour, 
  AVG(extra) AS avg_surcharge_by_hour,
  COUNT(vendor_id) AS ride_cnt_by_hour
FROM rides_fact rf
INNER JOIN datetime_dim dd
ON rf.pickup_datetime_id = dd.datetime_id
WHERE rf.extra >= 0
GROUP BY dd.hour
ORDER BY ride_cnt_by_hour DESC
;