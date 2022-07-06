# Nested Data

# Query to count the number of transactions per browser
query = """
        SELECT device.browser AS device_browser,
            SUM(totals.transactions) AS total_transactions
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
        GROUP BY device_browser
        ORDER BY total_transactions DESC
        """

# Who had the most commits in 2016?
max_commits_query = """
                    SELECT committer.name AS committer_name, COUNT(*) AS num_commits
                    FROM `bigquery-public-data.github_repos.sample_commits`
                    WHERE committer.date >= '2016-01-01' AND committer.date < '2017-01-01'
                    GROUP BY committer_name
                    ORDER BY num_commits DESC
                    """

# Nested and Repeated Data

# Query to determine most popular landing point on the website
query = """
        SELECT hits.page.pagePath AS path,
            COUNT(hits.page.pagePath) AS counts
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`, 
            UNNEST(hits) AS hits
        WHERE hits.type="PAGE" AND hits.hitNumber=1
        GROUP BY path
        ORDER BY counts DESC
        """

# What's the most popular programming language?
pop_lang_query = """
                 SELECT l.name AS language_name, COUNT(*) AS num_repos
                 FROM `bigquery-public-data.github_repos.languages`,
                     UNNEST(language) AS l
                 GROUP BY language_name
                 ORDER BY num_repos DESC
                 """

# Which languages are used in the repository with the most languages?
all_langs_query = """
                  SELECT l.name, l.bytes
                  FROM `bigquery-public-data.github_repos.languages`,
                      UNNEST(language) AS l
                  WHERE repo_name = 'polyrabbit/polyglot'
                  ORDER BY l.bytes DESC
                  """
