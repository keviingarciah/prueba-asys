from dotenv import load_dotenv
import boto3
import os
from botocore.exceptions import ClientError
import pandas as pd
import io
from sqlalchemy import create_engine, text
import numpy as np

# Load environment variables
load_dotenv()

def read_netflix_data_from_s3():
    """"Read Netflix data from S3 and return as a pandas DataFrame"""
    try:
        # Get credentials from .env
        access_key = os.getenv('S3_ACCES_KEY')
        secret_key = os.getenv('S3_SECRET_KEY')
        bucket_name = os.getenv('S3_BUCKET_NAME')
        file_key = os.getenv('S3_FILE_KEY')

        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        
        try:
            # Get the CSV file from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            
            # Read the CSV content
            csv_content = response['Body'].read()
            
            # Convert to pandas DataFrame
            df = pd.read_csv(io.BytesIO(csv_content))
            
            print(f"✅ Successfully read netflix_titles.csv")
            print("\nFirst 5 rows of the dataset:")
            print(df.head())
            print("\nDataset shape:", df.shape)
            
            return df
            
        except s3_client.exceptions.NoSuchKey:
            print(f"❌ File {file_key} not found in bucket!")
            return None
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == '404':
            print(f"❌ Bucket {bucket_name} does not exist!")
        elif error_code == '403':
            print(f"❌ Access denied to bucket {bucket_name}!")
        else:
            print("❌ Failed to connect to AWS S3!")
            print(f"Error: {e}")
        return None

def drop_all_tables(engine):
    """Drop all tables in the correct order to handle foreign key constraints"""
    try:
        with engine.connect() as conn:
            #* Disable foreign key checks temporarily
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0")) # This disables foreign key checks for the session
            
            # List of tables in order of deletion
            tables = [
                'titles_directors',
                'titles_categories',
                'titles_countries',
                'titles',
                'directors',
                'categories',
                'countries',
                'netflix_titles'
            ]
            
            # Drop each table
            for table in tables:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                print(f"✅ Table {table} dropped successfully")
            
            #* Re-enable foreign key checks
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1")) # This re-enables foreign key checks for the session
            conn.commit()
            
            print("✅ All tables dropped successfully")
            
    except Exception as e:
        print(f"❌ Error dropping tables: {e}")
        #* Ensure foreign key checks are re-enabled even if there's an error
        with engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
            conn.commit()
        raise e

def create_tables(engine):
    """Create all necessary tables in the database"""
    try:
        with engine.connect() as conn:
            #* Create pivot table first
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS netflix_titles (
                    show_id VARCHAR(10) PRIMARY KEY,
                    type VARCHAR(10),
                    title VARCHAR(255) NOT NULL,
                    director TEXT,
                    cast TEXT,
                    country TEXT,
                    date_added DATE,
                    release_year INT,
                    rating VARCHAR(10),
                    duration VARCHAR(20),
                    listed_in TEXT,
                    description TEXT
                )
            """))
        
            # Create titles table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS titles (
                    show_id VARCHAR(10) PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    type VARCHAR(10),
                    release_year INT,
                    rating VARCHAR(10),
                    duration VARCHAR(20),
                    date_added DATE,
                    description TEXT
                )
            """))
            
            # Create directors table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS directors (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL UNIQUE
                )
            """))
            
            # Create titles_directors table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS titles_directors (
                    show_id VARCHAR(10),
                    director_id INT,
                    PRIMARY KEY (show_id, director_id),
                    FOREIGN KEY (show_id) REFERENCES titles(show_id),
                    FOREIGN KEY (director_id) REFERENCES directors(id)
                )
            """))
            
            # Create categories table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS categories (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE
                )
            """))
            
            # Create titles_categories table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS titles_categories (
                    show_id VARCHAR(10),
                    category_id INT,
                    PRIMARY KEY (show_id, category_id),
                    FOREIGN KEY (show_id) REFERENCES titles(show_id),
                    FOREIGN KEY (category_id) REFERENCES categories(id)
                )
            """))
            
            # Create countries table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS countries (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE
                )
            """))
            
            # Create titles_countries table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS titles_countries (
                    show_id VARCHAR(10),
                    country_id INT,
                    PRIMARY KEY (show_id, country_id),
                    FOREIGN KEY (show_id) REFERENCES titles(show_id),
                    FOREIGN KEY (country_id) REFERENCES countries(id)
                )
            """))
            
            # Commit the transaction
            conn.commit()
            
            print("✅ Tables created successfully")
            
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        raise e

def process_and_upload_data(df, engine):
    """Process and upload data to normalized tables"""
    try:
        # Clean date format
        df['date_added'] = pd.to_datetime(
            df['date_added'].fillna('').astype(str),
            format='mixed',
            errors='coerce'
        ).dt.date
        
        #* Upload pivot table
        df.to_sql('netflix_titles', engine, if_exists='replace', index=False)
        print("✅ Pivot table netflix_titles updated")
        
        #* Populate normalized tables from pivot table
        with engine.connect() as conn:
            # Populate titles table
            conn.execute(text("""
                INSERT INTO titles (
                    show_id, title, type, release_year, 
                    rating, duration, date_added, description
                )
                SELECT 
                    show_id, title, type, release_year,
                    rating, duration, date_added, description
                FROM netflix_titles
            """))
            print("✅ titles table updated")
            
            # Process directors table using recursive CTE
            conn.execute(text("""
                INSERT IGNORE INTO directors (name)
                WITH RECURSIVE split_directors AS (
                    SELECT 
                        show_id,
                        SUBSTRING_INDEX(director, ',', 1) as name,
                        CONCAT(
                            CASE 
                                WHEN LOCATE(',', director) > 0 
                                THEN SUBSTRING(director, LOCATE(',', director) + 1)
                                ELSE ''
                            END
                        ) as remainder,
                        1 as level
                    FROM netflix_titles
                    WHERE director IS NOT NULL AND director != ''
                    
                    UNION ALL
                    
                    SELECT
                        show_id,
                        SUBSTRING_INDEX(remainder, ',', 1) as name,
                        CONCAT(
                            CASE 
                                WHEN LOCATE(',', remainder) > 0 
                                THEN SUBSTRING(remainder, LOCATE(',', remainder) + 1)
                                ELSE ''
                            END
                        ) as remainder,
                        level + 1
                    FROM split_directors
                    WHERE remainder != ''
                )
                SELECT DISTINCT TRIM(name) 
                FROM split_directors 
                WHERE TRIM(name) != ''
            """))
            print("✅ Directors table updated")
            
            # Process titles_directors table using recursive CTE
            conn.execute(text("""
                INSERT INTO titles_directors (show_id, director_id)
                WITH RECURSIVE split_directors AS (
                    SELECT 
                        show_id,
                        SUBSTRING_INDEX(director, ',', 1) as name,
                        CONCAT(
                            CASE 
                                WHEN LOCATE(',', director) > 0 
                                THEN SUBSTRING(director, LOCATE(',', director) + 1)
                                ELSE ''
                            END
                        ) as remainder,
                        1 as level
                    FROM netflix_titles
                    WHERE director IS NOT NULL AND director != ''
                    
                    UNION ALL
                    
                    SELECT
                        show_id,
                        SUBSTRING_INDEX(remainder, ',', 1) as name,
                        CONCAT(
                            CASE 
                                WHEN LOCATE(',', remainder) > 0 
                                THEN SUBSTRING(remainder, LOCATE(',', remainder) + 1)
                                ELSE ''
                            END
                        ) as remainder,
                        level + 1
                    FROM split_directors
                    WHERE remainder != ''
                )
                SELECT DISTINCT 
                    sd.show_id,
                    d.id as director_id
                FROM split_directors sd
                JOIN directors d ON TRIM(sd.name) = d.name
                WHERE TRIM(sd.name) != ''
            """))
            print("✅ Show-Directors relationships updated")
            
            # Process categories table
            conn.execute(text("""
                INSERT IGNORE INTO categories (name)
                SELECT DISTINCT TRIM(category) 
                FROM netflix_titles 
                CROSS JOIN JSON_TABLE(
                    CONCAT('["', REPLACE(listed_in, ',', '","'), '"]'),
                    '$[*]' COLUMNS (category VARCHAR(100) PATH '$')
                ) cats
                WHERE listed_in IS NOT NULL AND listed_in != ''
            """))
            print("✅ Categories table updated")
            
            # Process titles_categories table
            conn.execute(text("""
                INSERT INTO titles_categories (show_id, category_id)
                SELECT DISTINCT nt.show_id, c.id
                FROM netflix_titles nt
                CROSS JOIN JSON_TABLE(
                    CONCAT('["', REPLACE(nt.listed_in, ',', '","'), '"]'),
                    '$[*]' COLUMNS (category VARCHAR(100) PATH '$')
                ) cats
                JOIN categories c ON TRIM(cats.category) = c.name
                WHERE nt.listed_in IS NOT NULL AND nt.listed_in != ''
            """))
            print("✅ Show-Categories relationships updated")
            
            # Process countries table
            conn.execute(text("""
                INSERT IGNORE INTO countries (name)
                SELECT DISTINCT TRIM(country_name) 
                FROM netflix_titles 
                CROSS JOIN JSON_TABLE(
                    CONCAT('["', REPLACE(country, ',', '","'), '"]'),
                    '$[*]' COLUMNS (country_name VARCHAR(100) PATH '$')
                ) countries
                WHERE country IS NOT NULL AND country != ''
            """))
            print("✅ Countries table updated")
            
            # Process titles_countries table
            conn.execute(text("""
                INSERT INTO titles_countries (show_id, country_id)
                SELECT DISTINCT nt.show_id, c.id
                FROM netflix_titles nt
                CROSS JOIN JSON_TABLE(
                    CONCAT('["', REPLACE(nt.country, ',', '","'), '"]'),
                    '$[*]' COLUMNS (country_name VARCHAR(100) PATH '$')
                ) countries
                JOIN countries c ON TRIM(countries.country_name) = c.name
                WHERE nt.country IS NOT NULL AND nt.country != ''
            """))
            print("✅ Show-Countries relationships updated")
            
            conn.commit()
            print("✅ All normalized tables updated successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Error processing and uploading data: {e}")
        print(f"Error details: {str(e)}")
        return False

def upload_to_mysql(df):
    """Upload data to MySQL"""
    try:
        #* Get credentials from .env
        user = os.getenv('DATABASE_USER')
        password = os.getenv('DATABASE_PASSWORD')
        host = os.getenv('DATABASE_HOST')
        schema = os.getenv('DATABASE_SCHEMA')
        
        #* Create connection
        connection_string = f"mysql+pymysql://{user}:{password}@{host}/{schema}"
        engine = create_engine(connection_string)

        #* Drop existing tables
        drop_all_tables(engine)
        
        #* Create tables and upload data
        create_tables(engine)
        success = process_and_upload_data(df, engine)
        
        return success
        
    except Exception as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return False


if __name__ == "__main__":
    #* Read data from S3
    df = read_netflix_data_from_s3()
    
    #* If data was successfully read, upload to MySQL
    if df is not None:
        upload_to_mysql(df)