from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import re
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
CORS(app)

# Database configuration
DB_CONFIG = {
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'database': os.getenv('POSTGRES_DB'),
}


# Initialize Selenium WebDriver
def init_webdriver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=chrome_options)


# List of keywords to search for jobs
KEYWORDS = ['php developer', 'software engineer', 'full stack developer', 'backend developer', 'frontend developer',
            'mern stack developer', 'react developer', 'Laravel developer', 'nodejs developer',
            'javascript developer']  # Add more keywords


# Function to dynamically generate URLs based on keywords
def generate_urls():
    base_url = "https://www.linkedin.com/jobs/search/?currentJobId=4023652314&f_TPR=r86400&f_WT=2&geoId=103644278&keywords={keyword}&origin=JOB_SEARCH_PAGE_SEARCH_BUTTON&refresh=true"
    return [base_url.format(keyword=keyword.replace(' ', '%20')) for keyword in KEYWORDS]


# Function to scrape data and insert it into the database
def scrape_and_insert_data():
    urls = generate_urls()

    # Database connection
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO linkedin (job_title, job_link, company_name, company_link, job_source, job_location, salary, job_type, job_description, job_posted_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Loop through each URL and scrape job data
    for url in urls:
        driver = init_webdriver()
        driver.get(url)
        driver.implicitly_wait(10)
        driver.execute_script("window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });")
        time.sleep(3)
        html = driver.page_source
        driver.quit()

        soup = BeautifulSoup(html, "html.parser")
        section = soup.find("section", class_="two-pane-serp-page__results-list")
        if section is None:
            print(f"Section not found for URL: {url}. The structure might have changed.")
            continue

        job_sections = section.find_all("div",
                                        class_="base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card")

        for job_section in job_sections:
            job_title = job_section.find("h3", class_="base-search-card__title").text.strip() if job_section.find(
                "h3", class_="base-search-card__title") else "N/A"
            job_link_tag = job_section.find("a", class_="base-card__full-link")
            job_link = job_link_tag['href'].strip() if job_link_tag else "N/A"
            job_description_raw = job_section.find("div",
                                                   class_="show-more-less-html__markup relative overflow-hidden").text.strip() if job_section.find(
                "div", class_="show-more-less-html__markup relative overflow-hidden") else "N/A"
            job_description = re.sub(r'\s+', ' ', job_description_raw).strip()

            company_name = job_section.find("a", class_="hidden-nested-link").text.strip() if job_section.find("a",
                                                                                                               class_="hidden-nested-link") else "N/A"
            company_link_tag = job_section.find("a", class_="hidden-nested-link")
            company_link = company_link_tag['href'].strip() if company_link_tag else "N/A"
            job_source = "LinkedIn"
            job_type = job_section.find("li",
                                        class_="job-details-jobs-unified-top-card__job-insight job-details-jobs-unified-top-card__job-insight--highlight").text.strip() if job_section.find(
                "li",
                class_="job-details-jobs-unified-top-card__job-insight job-details-jobs-unified-top-card__job-insight--highlight") else "N/A"
            job_location = job_section.find("span",
                                            class_="job-search-card__location").text.strip() if job_section.find(
                "span", class_="job-search-card__location") else "N/A"
            salary = job_section.find("div", class_="salary compensation__salary").text.strip() if job_section.find(
                "div", class_="salary compensation__salary") else "N/A"
            job_posted_date = job_section.find("time",
                                               class_="job-search-card__listdate--new").text.strip() if job_section.find(
                "time", class_="job-search-card__listdate--new") else "N/A"

            data = (job_title, job_link, company_name, company_link, job_source, job_location, salary, job_type,
                    job_description, job_posted_date)

            try:
                cursor.execute(insert_query, data)
            except psycopg2.Error as err:
                print(f"Error inserting data: {err}")
                print(f"Failed data: {data}")

        print(f"Scraping completed for URL: {url}")

    conn.commit()
    cursor.close()
    conn.close()


# Scheduler function to run the scraping task every hour
def schedule_scraping_job():
    scheduler = BackgroundScheduler()
    scheduler.add_job(scrape_and_insert_data, 'interval', hours=1)  # Schedule the job to run every hour
    scheduler.start()


# API route to trigger scraping manually
@app.route('/scrape', methods=['POST'])
def scrape():
    scrape_and_insert_data()
    return jsonify({"status": "success", "message": "Data scraped and inserted successfully."})


# API to retrieve jobs with pagination and search
@app.route('/api/jobs', methods=['POST'])
def get_jobs():
    data = request.get_json() or {}
    search_query = data.get('search', '')
    page = data.get('page', 1)
    per_page = data.get('limit', 10)

    page = int(page)
    per_page = int(per_page)
    offset = (page - 1) * per_page

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    search_query = f"%{search_query}%"

    query = """
                SELECT * FROM linkedin
                WHERE job_title LIKE %s OR job_link LIKE %s OR company_name LIKE %s OR company_link LIKE %s OR job_source LIKE %s OR job_location LIKE %s OR salary LIKE %s OR job_type LIKE %s OR job_description LIKE %s OR job_posted_date LIKE %s
                LIMIT %s OFFSET %s
            """
    cursor.execute(query, (
        search_query, search_query, search_query, search_query, search_query, search_query, search_query, search_query,
        search_query, search_query, per_page, offset))

    jobs = cursor.fetchall()

    cursor.execute(
        "SELECT COUNT(*) as total FROM linkedin WHERE job_title LIKE %s OR job_link LIKE %s OR company_name LIKE %s OR company_link LIKE %s OR job_source LIKE %s OR job_location LIKE %s OR salary LIKE %s OR job_type LIKE %s OR job_description LIKE %s OR job_posted_date LIKE %s",
        (search_query, search_query, search_query, search_query, search_query, search_query, search_query,
         search_query, search_query, search_query))
    total_jobs = cursor.fetchone()['total']

    response = {
        'jobs': jobs,
        'page': page,
        'per_page': per_page,
        'total_jobs': total_jobs,
        'total_pages': (total_jobs // per_page) + (1 if total_jobs % per_page > 0 else 0)
    }

    cursor.close()
    conn.close()

    return jsonify(response)


if __name__ == "__main__":
    schedule_scraping_job()  # Start the cron job
    app.run(port=5000)
