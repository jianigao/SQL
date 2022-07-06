# INNER JOIN

answers_query ="""
                SELECT a.id, a.body, a.owner_user_id
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q 
                INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                    ON q.id = a.parent_id
                WHERE q.tags LIKE '%bigquery%'
                """

bigquery_experts_query = """
                         SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
                         FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                         INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                             ON q.id = a.parent_Id
                         WHERE q.tags LIKE '%bigquery%'
                         GROUP BY a.owner_user_id
                         """

# LEFT JOIN

# Query to select all stories posted on January 1, 2012, with number of comments
join_query = """
             WITH c AS
             (
             SELECT parent, COUNT(*) AS num_comments
             FROM `bigquery-public-data.hacker_news.comments` 
             GROUP BY parent
             )
             SELECT s.id AS story_id, s.by, s.title, c.num_comments
             FROM `bigquery-public-data.hacker_news.stories` AS s
             LEFT JOIN c
                ON s.id = c.parent
             WHERE EXTRACT(DATE FROM s.time_ts) = '2012-01-01'
             ORDER BY c.num_comments DESC
             """

# How long does it take for questions to receive answers?
correct_query = """
                SELECT q.id AS q_id,
                    MIN(TIMESTAMP_DIFF(a.creation_date, q.creation_date, SECOND)) as time_to_answer
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                    LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                ON q.id = a.parent_id
                WHERE q.creation_date >= '2018-01-01' and q.creation_date < '2018-02-01'
                GROUP BY q_id
                ORDER BY time_to_answer
                """

# Track users who joined the site in January 2019
# when did they post their first questions and answers, if ever?
three_tables_query = """
                     SELECT u.id AS id,
                         MIN(q.creation_date) AS q_creation_date,
                         MIN(a.creation_date) AS a_creation_date
                     FROM `bigquery-public-data.stackoverflow.users` AS u
                         LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                             ON u.id = a.owner_user_id
                         LEFT JOIN `bigquery-public-data.stackoverflow.posts_questions` AS q
                             ON q.owner_user_id = u.id
                     WHERE u.creation_date >= '2019-01-01' and u.creation_date < '2019-02-01'
                     GROUP BY id
                     """

# FULL JOIN

# Is it more common for users to first ask questions or provide answers?
# After signing up, how long does it take for users to first interact with the website?
q_and_a_query = """
                SELECT q.owner_user_id AS owner_user_id,
                    MIN(q.creation_date) AS q_creation_date,
                    MIN(a.creation_date) AS a_creation_date
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                    FULL JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                ON q.owner_user_id = a.owner_user_id 
                WHERE q.creation_date >= '2019-01-01' AND q.creation_date < '2019-02-01' 
                    AND a.creation_date >= '2019-01-01' AND a.creation_date < '2019-02-01'
                GROUP BY owner_user_id
                """

# UNION

# Query to select all users who posted stories or comments on January 1, 2014
union_query = """
              SELECT c.by
              FROM `bigquery-public-data.hacker_news.comments` AS c
              WHERE EXTRACT(DATE FROM c.time_ts) = '2014-01-01'
              UNION DISTINCT
              SELECT s.by
              FROM `bigquery-public-data.hacker_news.stories` AS s
              WHERE EXTRACT(DATE FROM s.time_ts) = '2014-01-01'
              """
