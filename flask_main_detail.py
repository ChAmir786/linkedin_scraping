from flask import Flask, jsonify
import time
import psycopg2
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)


# Function to initialize the Selenium driver
def init():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-webrtc")
    chrome_options.add_argument("--disable-rtc-smoothness-algorithm")
    chrome_options.add_argument("--disable-rtc-smoothing")

    return webdriver.Chrome(options=chrome_options)


# Function to crawl a page and return its content
def crawlPage(driver, url):
    driver.get(url)
    driver.implicitly_wait(10)
    time.sleep(3)
    return driver.page_source


# Function to parse the HTML using BeautifulSoup
def parseHtml(html):
    return BeautifulSoup(html, "html.parser")


# Function to extract additional job data
def extractAdditionalJobData(soup):
    job_description = (soup.find("div", class_="description__text description__text--rich")
                       .text.strip()) if soup.find("div", class_="description__text description__text--rich") else "N/A"
    job_type = (soup.find("span", class_="job-details-jobs-unified-top-card__job-insight-view-model-secondary")
                .text.strip()) if soup.find("span",
                                            class_="job-details-jobs-unified-top-card__job-insight-view-model-secondary") else "N/A"
    salary = (soup.find("div", class_="salary compensation__salary")
              .text.strip()) if soup.find("div", class_="salary compensation__salary") else "N/A"

    return {
        "JobDescription": job_description,
        "JobType": job_type,
        "Salary": salary
    }


# Function to update the job data in the Postgres database
def update_job_data(cursor, data, job_id):
    try:
        update_query = """
        UPDATE linkedin 
        SET job_description = %s, job_type = %s, salary = %s
        WHERE id = %s
        """
        cursor.execute(update_query, (
            data['JobDescription'],
            data['JobType'],
            data['Salary'],
            job_id
        ))
    except psycopg2.Error as err:
        print(f"Error updating data: {err}")
        print(f"Failed data: {data}")


# Main function that handles scraping and updating jobs
def scrape_and_update_jobs():
    # Database connection parameters
    config = {
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
        'database': os.getenv('POSTGRES_DB'),
    }

    # Initialize the web driver
    driver = init()

    # Initialize the connection and cursor variables
    conn = None
    cursor = None

    try:
        # Connect to the database
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()

        offset = 0
        limit = 10

        while True:
            # Fetch records with offset and limit
            cursor.execute("SELECT id, job_link FROM linkedin LIMIT %s OFFSET %s", (limit, offset))
            jobs = cursor.fetchall()

            print('Jobs: ', offset)
            # Break the loop if no more records are fetched
            if not jobs:
                break

            for job in jobs:
                job_id, job_link = job

                # Crawl the job link page
                html = crawlPage(driver, job_link)
                soup = parseHtml(html)

                # Extract additional job data
                additional_data = extractAdditionalJobData(soup)

                # Update the job record in the database with the new data
                update_job_data(cursor, additional_data, job_id)

            # Commit the transaction for the current batch
            conn.commit()

            # Increment offset for the next batch
            offset += limit

    except psycopg2.Error as err:
        print(f"Database Error: {err}")

    finally:
        # Close the cursor and connection if they were initialized
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
        driver.quit()


# Flask route to trigger the scraping and updating process
@app.route('/scrape-jobs', methods=['POST'])
def scrape_jobs():
    try:
        scrape_and_update_jobs()
        return jsonify({'message': 'Jobs scraped and updated successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
