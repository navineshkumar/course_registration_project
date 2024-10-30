import os
import pandas as pd
import sqlalchemy

# Load database credentials from environment variables
DB_NAME = os.getenv('DB_NAME')  # Database name
DB_USER = os.getenv('DB_USER')  # Database username
DB_PASSWORD = os.getenv('DB_PASSWORD')  # Database password
DB_HOST = os.getenv('DB_HOST', 'localhost')  # Database host (default: localhost)
DB_PORT = os.getenv('DB_PORT', '5432')  # Database port (default: 5432 for PostgreSQL)

# Create a connection string
connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Create an SQLAlchemy engine
engine = sqlalchemy.create_engine(connection_string)

# SQL query to join the specified tables
query = """
SELECT 
    ac.program AS Program,
    ac.email AS Email,
    c.course_name AS Course_name,
    t.applying_level AS Applying_level,
    ac.current_level AS Current_level,
    c.fee_details AS Fee_details,
    a.application_status AS Application_status
FROM 
    TermCourseRegistrationApplication t
JOIN 
    Application a ON t.application_id = a.id
JOIN 
    Account ac ON a.account_id = ac.id
JOIN 
    Course c ON t.course_id = c.id;
"""

# Execute the query and return the result as a DataFrame
try:
    df = pd.read_sql(query, engine)
    print("Data retrieved successfully.")
except Exception as e:
    print("An error occurred:", e)
finally:
    # Close the engine connection
    engine.dispose()

# Display the DataFrame
print(df)
