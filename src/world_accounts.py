# Import library
import psycopg2
import pandas as pd

# Creates a database
def create_database():
	# connect to default database
	conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=root")
	conn.set_session(autocommit=True)
	cur = conn.cursor()

	# create sparkify database with UTF8 encoding
	cur.execute("DROP DATABASE accounts")
	cur.execute("CREATE DATABASE accounts")

	# close connection to default database
	conn.close()

	# connect to sparkify database
	conn = psycopg2.connect("host=127.0.0.1 dbname=accounts user=postgres password=root")
	cur = conn.cursor()

	return cur, conn

def drop_tables(cur, conn):
	for query in drop_table_queries:
		cur.execute(query)
		conn.commit()

def create_tables(cur, conn):
	for query in create_table_queries:
		cur.execute(query)
		conn.commit()

account_country = pd.read_csv("data/wealth_account_country.csv")
account_country.head()
account_country_clean = account_country[['Country Name', 'Country Code', 'Series Name', 'Series Code', '2017 [YR2017]', '2018 [YR2018]']]
account_country_clean.head()
account_series = pd.read_csv("/content/series.csv")
account_series.head()
account_series.columns
# To drop a column 
account_series = account_series.drop(['Previous Indicator Name'], axis=1)
account_series.head()
account_series_clean = account_series[['Code', 'Indicator Name', 'Long definition', 
                                          'Source', 'Topic', 'Unit of measure', 'Statistical concept and methodology']]
accounts_series_clean.head()
cur, conn = create_database()

# Create a table
account_country_table = ("""CREATE TABLE IF NOT EXISTS account_country(
							country_name VARCHAR PRIMARY KEY, 
							country_code VARCHAR,
							series_name VARCHAR,
							series_code VARCHAR,
							year_2017 numeric,
							year_2018 numeric) """)
cur.execute(account_country_table)
conn.commit()

account_series_table = ("""CREATE TABLE IF NOT EXISTS account_series(
							code VARCHAR PRIMARY KEY, 
							indicator_name VARCHAR,
							long_definition VARCHAR,
							source VARCHAR,
							topic VARCHAR,
							unit_of_measure VARCHAR,
							concept_and_methodology VARCHAR) """)

cur.execute(account_series_table)
conn.commit()

# Insert data into database
account_country_table_insert = (""" INSERT INTO account_country(
									country_name,
									country_code,
									series_name,
									year_2017,
									year_2018)
									VALUES (%s, %s, %s, %s, %s)""")

# Load data into the database
for i, row in account_country_clean.iterrows():
	cur.execute(account_country_table_insert, list(row))

conn.commit()

account_series_table_insert = (""" INSERT INTO account_country(
									code,
									indicator_name,
									long_definition,
									source,
									topic,
									unit_of_measure,
									concept_and_methodology)
									VALUES (%s, %s, %s, %s, %s, %s, %s)""")

for i, row in account_series_clean.iterrows():
	cur.execute(account_series_table_insert, list(row))

conn.commit()