import mysql.connector

def apply_migration():
    # Database connection parameters
    config = {
        'user': 'root',
        'password': '1234567',
        'host': 'localhost',
        'port': 3306,  # Specify the port separately
        'database': 'web_scraping'
    }

    # SQL query to create the table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS linkedin (
        id INT AUTO_INCREMENT PRIMARY KEY,
        job_title VARCHAR(255),
        job_link TEXT,
        company_name VARCHAR(255),
        company_link TEXT,
        job_source VARCHAR(255),
        job_location VARCHAR(255),
        salary VARCHAR(255),
        job_type VARCHAR(255),
        job_description TEXT,
        job_posted_date VARCHAR(50)
    )
    """

    conn = None
    cursor = None

    try:
        # Connect to the database
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # Execute migration commands
        cursor.execute(create_table_query)

        # Commit changes
        conn.commit()

        print("Table 'linkedin' created or already exists.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close the cursor and connection if they were initialized
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    apply_migration()