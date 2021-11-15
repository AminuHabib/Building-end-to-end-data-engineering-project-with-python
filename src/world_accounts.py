import psycopg2
import pandas as pd

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
	for query in create_database drop_table_queries:
		cur.execute(quer)
		conn.commit()

def create_tables(cur, conn):
	for query in create_table_queries:
		cur.execute(query)
		conn.commit()

accounts_country = pd.read_csv("data/wealth_account_country.csv")
accounts_country.head()
accounts_country_clean = accounts_country[['Country Name', 'Country Code', 'Series Name', 'Series Code', '2017 [YR2017]', '2018 [YR2018]']]
accounts_country_clean.head()
account_series = pd.read_csv("/content/series.csv")
account_series.head()
account_series.columns