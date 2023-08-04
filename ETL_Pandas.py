import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('S30 ETL Assignment.db')

# Define the SQL query
query_customer = "SELECT * FROM customers"
query_sales = "SELECT * FROM sales"
query_orders = "SELECT * FROM orders"
query_items = "SELECT * FROM items"

# Read data into a pandas DataFrame
df_customer = pd.read_sql_query(query_customer, conn)
df_sales = pd.read_sql_query(query_sales, conn)
df_orders = pd.read_sql_query(query_orders, conn)
df_items = pd.read_sql_query(query_items, conn)

# Close connection
conn.close()

merged_df = df_customer.merge(df_sales, on='customer_id') \
    .merge(df_orders, on='sales_id') \
    .merge(df_items, on='item_id')

# Filter customers aged 18-35
filtered_df = merged_df[(merged_df['age'] >= 18) & (merged_df['age'] <= 35)]

# Group by and aggregate
result_df = filtered_df.groupby(['customer_id', 'age', 'item_name'])['quantity'].sum().round().astype(int) \
    .reset_index(name='Quantity') \
    .query('Quantity > 0') \
    .sort_values(by=['customer_id'])

# Rename columns
result_df.rename(columns={'customer_id': 'Customer', 'age': 'Age', 'item_name': 'Item'}, inplace=True)
print(result_df)

# Generate CSV
result_df.to_csv('exam_output_pandas.csv', index=False, sep=';')
