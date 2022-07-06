from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# List all the tables in the dataset
tables = list(client.list_tables(dataset))

# Print number of tables in the dataset
print(len(tables))

# Get a list of available tables 
list_of_tables = [table.table_id for table in tables]

# Print names of all tables in the dataset
for table in tables:  
    print(table.table_id)

# Construct a reference to the "comments" table
table_ref = dataset_ref.table("comments")

# API request - fetch the table
table = client.get_table(table_ref)

# Print information on all the columns
print(table.schema)

# Print information on the second columns
print(table.schema[1])

# Preview the first five lines of the "comments" table
client.list_rows(table, max_results=5).to_dataframe()

# Preview the first five entries in the second column of the table
client.list_rows(table, selected_fields=table.schema[:1], max_results=5).to_dataframe()

# Query to select prolific commenters and post counts
prolific_commenters_query = """
                            SELECT author, COUNT(1) AS NumPosts
                            FROM `bigquery-public-data.hacker_news.comments`
                            GROUP BY author
                            HAVING COUNT(1) > 10000
                            """

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**9)
query_job = client.query(prolific_commenters_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
prolific_commenters = query_job.to_dataframe()
# or
prolific_commenters = query_job.result().to_dataframe()

# View top few rows of results
print(prolific_commenters.head())
