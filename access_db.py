import sqlite3

# Connect to the listings.db file
conn = sqlite3.connect('listings.db')  # Ensure the path is correct

# Create a cursor object
cur = conn.cursor()

# Execute an SQL query (example: select all records from listings table)
cur.execute("SELECT * FROM listings")

# Fetch all results from the executed query
rows = cur.fetchall()

# Print the fetched data
for row in rows:
    print(row)

# Close the connection
conn.close()