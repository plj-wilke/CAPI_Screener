import pandas as pd
import psycopg2
df = pd.read_csv("df.csv")


schema_df = pd.read_csv("schema_df.csv")#, index_col="responseid")

print(schema_df)


conn_details = psycopg2.connect(
   host="localhost",
   database="postgres",
   user="postgres",
   password="*****",
   port= '5432'
)

cursor = conn_details.cursor()
Table_creation = '''
   CREATE TABLE staff_information (
       stf_id SERIAL PRIMARY KEY,
       stf_name TEXT NOT NULL
   )
'''
cursor.execute(table_creation)