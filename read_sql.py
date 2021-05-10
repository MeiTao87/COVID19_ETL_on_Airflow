import sqlite3

# Create a SQL connection to our SQLite database
con = sqlite3.connect("./covid19.sqlite")

cur = con.cursor()
i = 0
# The result of a "cursor.execute" can be iterated over by row
for row in cur.execute('SELECT Combined_Key, Last_Update FROM covid19 ORDER BY Last_Update;'):
    print(row)
    i += 1
print(i)

# Be sure to close the connection
con.close()


# docker run -d --name my_postgres -v my_dbdata:/var/lib/postgresql/data -p 54320:5432 postgres:11
