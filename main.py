"""Entry point for the ETL application

Sample usage:
docker-compose run etl python main.py \
    --source /opt/data/activity.csv \
    --database warehouse
    --table user_activity
"""
# TODO: Implement a pipeline that loads the provided activity.csv file, performs the required
# transformations, and loads the result into the PostgreSQL table.

# Note: You can write the ETL flow with regular Python code, or you can also make use of a
# framework such as PySpark or others. The choice is yours.

import psycopg2
import pandas as pd
from argparse import ArgumentParser

def get_args():
  parser = ArgumentParser()
  parser.add_argument('--source')
  parser.add_argument('--database')
  parser.add_argument('--table')
  return parser.parse_args()

def extract_data(filepath):
  return pd.read_csv(filepath)

def transform_data(df):
  df = df.groupby('user_id').agg({
    'workspace_id': 'last', 
    'total_activity': 'sum'
  }).reset_index()
  
  df['longest_streak'] = (df['date'] 
                          .groupby(df['user_id'])
                          .apply(lambda x: (x!=x.shift()).cumsum())
                          .groupby(df['user_id']).max())

  return df  

def load_data(df, db, table):
  conn = psycopg2.connect(database=db, user='postgres', password='postgres', host='db')
  cursor = conn.cursor()

  columns = ','.join(df.columns)
  values = "VALUES({})".format(",".join(["%s" for _ in df.columns])) 
  insert_stmt = f"INSERT INTO {table} ({columns}) {values}"

  cursor.executemany(insert_stmt, df.values.tolist())
  conn.commit()

  conn.close()

if __name__ == "__main__":
  args = get_args()
  df = extract_data(args.source)
  df = transform_data(df) 
  load_data(df, args.database, args.table)