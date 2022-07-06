'''
All analytic functions have an OVER clause, which defines the sets of 
rows used in each calculation. The OVER clause has three (optional) parts:

1) The PARTITION BY clause divides the rows of the table into different groups.
2) The ORDER BY clause defines an ordering within each partition. 
3) The final clause (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) is known as 
a window frame clause. It identifies the set of rows used in each calculation. 
'''

'''
There are many ways to write window frame clauses:

1) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW - the previous row and 
the current row.
2) ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING - the 3 previous rows, 
the current row, and the following row.
3) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING - all 
rows in the partition.
'''

# Three types of analytic functions:

# 1) Analytic aggregate functions
# AVG(), MIN(), MAX(), AVG(), SUM(), COUNT()

# 2) Analytic navigation functions
# FIRST_VALUE(), LAST_VALUE()
# LEAD() or LAG(), returns the value on a subsequent or preceding row

# 3) Analytic numbering functions
# ROW_NUMBER() returns the order in which rows appear in the input (starting with 1)
# RANK() - All rows with the same value in the ordering column
#   receive the same rank value, where the next row receives a rank value
#   which increments by the number of rows with the previous rank value.


# Query to count the (cumulative) number of trips per day
num_trips_query = """
                  WITH trips_by_day AS
                  (
                  SELECT DATE(start_date) AS trip_date,
                      COUNT(*) as num_trips
                  FROM `bigquery-public-data.san_francisco.bikeshare_trips`
                  WHERE EXTRACT(YEAR FROM start_date) = 2015
                  GROUP BY trip_date
                  )
                  SELECT *,
                      SUM(num_trips) 
                          OVER (
                               ORDER BY trip_date
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                               ) AS cumulative_trips
                      FROM trips_by_day
                  """

# Query to track beginning and ending stations on October 25, 2015, for each bike
start_end_query = """
                  SELECT bike_number,
                      TIME(start_date) AS trip_time,
                      FIRST_VALUE(start_station_id)
                          OVER (
                               PARTITION BY bike_number
                               ORDER BY start_date
                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                               ) AS first_station_id,
                      LAST_VALUE(end_station_id)
                          OVER (
                               PARTITION BY bike_number
                               ORDER BY start_date
                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                               ) AS last_station_id,
                      start_station_id,
                      end_station_id
                  FROM `bigquery-public-data.san_francisco.bikeshare_trips`
                  WHERE DATE(start_date) = '2015-10-25' 
                  """

# Show a rolling average of the daily number of taxi trips
avg_num_trips_query = """
                      WITH trips_by_day AS
                      (
                      SELECT DATE(trip_start_timestamp) AS trip_date,
                          COUNT(*) AS num_trips
                      FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                      WHERE trip_start_timestamp >= '2016-01-01' AND trip_start_timestamp < '2018-01-01'
                      GROUP BY trip_date
                      ORDER BY trip_date
                      )
                      SELECT trip_date,
                          AVG(num_trips) 
                          OVER (
                               ORDER BY trip_date
                               ROWS BETWEEN 15 PRECEDING AND 15 FOLLOWING
                               ) AS avg_num_trips
                      FROM trips_by_day
                      """

# Show the order in which the trips were taken from their respective community areas
trip_number_query = """
                    SELECT pickup_community_area,
                        trip_start_timestamp,
                        trip_end_timestamp,
                        RANK()
                            OVER (
                                 PARTITION BY pickup_community_area
                                 ORDER BY trip_start_timestamp
                                 ) AS trip_number
                    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                    WHERE DATE(trip_start_timestamp) = '2017-05-01' 
                    """

# Show the length of the break (in minutes) that the driver had before each trip started
break_time_query = """
                   SELECT taxi_id,
                       trip_start_timestamp,
                       trip_end_timestamp,
                       TIMESTAMP_DIFF(
                           trip_start_timestamp, 
                           LAG(trip_end_timestamp, 1) 
                               OVER (
                                    PARTITION BY taxi_id 
                                    ORDER BY trip_start_timestamp), 
                           MINUTE) AS prev_break
                   FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                   WHERE DATE(trip_start_timestamp) = '2017-05-01' 
                   """
