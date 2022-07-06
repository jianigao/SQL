# Use SELECT to pull specific columns from a table,
# along with WHERE to pull rows that meet specified criteria.

# Select city column
query = """
        SELECT city
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """

# Select city and country column
query = """
        SELECT city, country
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """

# Query to select all columns where pollution levels are exactly 0
zero_pollution_query = """
              SELECT *
              FROM `bigquery-public-data.openaq.global_air_quality`
              WHERE value = 0
              """

# Query to select distinct countries with units of "ppm"
first_query = """
              SELECT DISTINCT country
              FROM `bigquery-public-data.openaq.global_air_quality`
              WHERE unit = "ppm"
              """

# aggregate functions: COUNT(), SUM(), AVG(), MIN(), and MAX()
# COUNT(column_name)
# COUNT(1) counts the rows in each group (excluding na)
# COUNT(*) counts the rows in each group (including na)
deleted_posts_query = """
                      SELECT COUNT(1) AS num_deleted_posts
                      FROM `bigquery-public-data.hacker_news.comments`
                      WHERE deleted = True
                      """

# Use aggregate functions, 
# along with GROUP BY to treat multiple rows as a single group.

# If you have any GROUP BY clause, then all variables must be passed 
# to either 1) a GROUP BY command, or 2) an aggregation function.

# Query to select prolific commenters and post counts
prolific_commenters_query = """
                            SELECT author, COUNT(1) AS NumPosts
                            FROM `bigquery-public-data.hacker_news.comments`
                            GROUP BY author
                            HAVING COUNT(1) > 10000
                            """

# Use ORDER BY to change the order of results.

# Query to find out the number of accidents for each day of the week
query = """
        SELECT COUNT(consecutive_number) AS num_accidents, 
               EXTRACT(DAYOFWEEK FROM timestamp_of_crash) AS day_of_week
        FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015`
        GROUP BY day_of_week
        ORDER BY num_accidents DESC
        """

# Which countries spend the largest fraction of GDP on education?
country_spend_pct_query = """
                          SELECT country_name, AVG(value) AS avg_ed_spending_pct
                          FROM `bigquery-public-data.world_bank_intl_education.international_education`
                          WHERE indicator_code = 'SE.XPD.TOTL.GD.ZS' and year >= 2010 and year <= 2017
                          GROUP BY country_name
                          ORDER BY avg_ed_spending_pct DESC
                          """

# Identify interesting codes to explore
code_count_query ="""
                   SELECT indicator_code, indicator_name, COUNT(1) AS num_rows
                   FROM `bigquery-public-data.world_bank_intl_education.international_education`
                   WHERE year = 2016
                   GROUP BY indicator_name, indicator_code
                   HAVING COUNT(1) >= 175
                   ORDER BY COUNT(1) DESC
                   """

# WITH ... AS
# A common table expression (or CTE) is a temporary table that you return 
# within your query. CTEs are helpful for splitting your queries into readable 
# chunks, and you can write queries against them.

# Query to select the number of transactions per date, sorted by date
query_with_CTE = """ 
                 WITH time AS 
                 (
                     SELECT DATE(block_timestamp) AS trans_date
                     FROM `bigquery-public-data.crypto_bitcoin.transactions`
                 )
                 SELECT trans_date, COUNT(1) AS transactions
                 FROM time
                 GROUP BY trans_date
                 ORDER BY trans_date
                 """
# DATE(), DATETIME() convert values to date/datetime format

rides_per_year_query = """
                       SELECT EXTRACT(YEAR FROM trip_start_timestamp) AS year, 
                              COUNT(1) AS num_trips
                       FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                       GROUP BY year
                       ORDER BY year
                       """

rides_per_month_query = """
                       SELECT EXTRACT(Month FROM trip_start_timestamp) AS month, 
                              COUNT(1) AS num_trips
                       FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                       WHERE EXTRACT(Year FROM trip_start_timestamp)=2017
                       GROUP BY month
                       ORDER BY month
                       """ 

speeds_query = """
               WITH RelevantRides AS
               (
                   SELECT EXTRACT(HOUR FROM trip_start_timestamp) AS hour_of_day, 
                          trip_miles, 
                          trip_seconds
                   FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                   WHERE trip_start_timestamp > '2017-01-01' AND 
                         trip_start_timestamp < '2017-07-01' AND 
                         trip_seconds > 0 AND 
                         trip_miles > 0
               )
               SELECT hour_of_day, 
                      COUNT(1) AS num_trips, 
                      3600 * SUM(trip_miles) / SUM(trip_seconds) AS avg_mph
               FROM RelevantRides
               GROUP BY hour_of_day
               ORDER BY hour_of_day
               """

# Query to determine the number of files per license, sorted by number of files
query = """
        SELECT L.license, COUNT(1) AS number_of_files
        FROM `bigquery-public-data.github_repos.sample_files` AS sf
        INNER JOIN `bigquery-public-data.github_repos.licenses` AS L 
            ON sf.repo_name = L.repo_name
        GROUP BY L.license
        ORDER BY number_of_files DESC
        """

# A WHERE clause can limit your results to rows with certain text using the LIKE feature. 
# use % as a "wildcard" for any number of characters.

questions_query = """
                  SELECT id, title, owner_user_id
                  FROM `bigquery-public-data.stackoverflow.posts_questions`
                  WHERE tags LIKE '%bigquery%'
                  """
