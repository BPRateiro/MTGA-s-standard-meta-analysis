import pandas as pd
from sqlite3 import connect

# Get connection and cursor
connection = connect('./untapped.db')
cursor = connection.cursor()

# Garantee that the table has autoincrement schema
create = \
"""
CREATE TABLE IF NOT EXISTS timestamp(
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    year    INT,
    month   INT,
    day     INT,
    hour    INT
);
"""
cursor.execute(create)

# Record timestamp
timestamp = pd.Timestamp.now()
cursor.execute(f"""
    INSERT INTO timestamp (year, month, day, hour) VALUES
        ({timestamp.year}, {timestamp.month}, {timestamp.day}, {timestamp.hour})
""")

# Commit changes and close connection
connection.commit()
connection.close()