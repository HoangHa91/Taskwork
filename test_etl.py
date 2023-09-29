import psycopg2
import pandas as pd
USER_ID = '5bfd0e8d472bcf0009a1014d'

try:
  conn = psycopg2.connect(host="postgres", dbname="warehouse", user="postgres", password="postgres")
  
  df = pd.read_sql(f'SELECT * FROM user_activity WHERE user_id=\'{USER_ID}\'', conn)

  assert df['top_workspace'].values[0] == '5bfd08d8326bcf0009a10cb5'
  assert df['longest_streak'].values[0] == 3
  
  print("Integration test passed!")
  
except psycopg2.Error as e:
  print("Connection error:")
  print(e)
  
finally:
  conn.close()