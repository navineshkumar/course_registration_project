import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection details from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Create the database connection URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Function to create a database connection
def create_db_connection():
    try:
        engine = create_engine(DATABASE_URL)
        connection = engine.connect()
        print("Connection successful")
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def fetch_data_in_chunks(connection, chunk_size=10000):
    query = """
    SELECT 
        acc.program AS "Program",
        acc.email AS "Email",
        course.course_name AS "Course Name",
        tcr.application_level AS "Applying Level",
        tcr.current_level AS "Current Level",
        course.fee_details AS "Fee Details",
        app.application_status AS "Application Status"
    FROM TermCourseRegistrationApplication tcr
    JOIN Application app ON tcr.application_id = app.application_id
    JOIN Account acc ON app.account_id = acc.account_id
    JOIN Course course ON tcr.course_id = course.course_id
    """
    
    try:
        chunks = pd.read_sql(query, connection, chunksize=chunk_size)
        df_list = []
        for chunk in chunks:
            df_list.append(chunk)
        df = pd.concat(df_list)
        print("Data fetched in chunks successfully")
        return df
    except Exception as e:
        print(f"Error executing chunked query: {e}")
        return None
df = fetch_course_registration_data(connection)
if df is not None:
    df.to_csv('course_registration_data.csv', index=False)
    print("Data saved to course_registration_data.csv")

