import csv
import sqlite3

# Connect to DB
db_connection = sqlite3.connect("S30 ETL Assignment.db")
cursor = db_connection.cursor()

# Execute SQL
query = "SELECT customers.customer_id as Customer,customers.age as Age,items.item_name as Item,COALESCE(SUM(" \
        "orders.quantity), 0) AS Quantity FROM customers JOIN sales ON customers.customer_id = sales.customer_id JOIN " \
        "orders ON sales.sales_id = orders.sales_id JOIN items ON orders.item_id = items.item_id WHERE customers.age " \
        "BETWEEN 18 AND 35 and Quantity > 0 GROUP BY customers.customer_id, customers.age, items.item_name  ORDER BY " \
        "customers.customer_id;"

cursor.execute(query)

# Fetch the results
results = cursor.fetchall()

# CSV file name
csv_file = "exam_output_sql.csv"

# Write results to CSV
with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_MINIMAL)

    # Write header
    csvwriter.writerow(['Customer', 'Age', 'Item', 'Quantity'])

    # Write data rows
    for row in results:
        csvwriter.writerow(row)

print(f'CSV file "{csv_file}" created successfully.')


db_connection.commit()
db_connection.close()
print("DB closed.")
