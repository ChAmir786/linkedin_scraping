from flask import Flask, jsonify, request
from flask_cors import CORS  # Import CORS
import mysql.connector
from mysql.connector import Error
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

app = Flask(__name__)
CORS(app)
# CORS(app, resources={r"/api/*": {"origins": "http://localhost:3003"}})
# app = Flask(__name__)

# Database configuration
DB_CONFIG = {
    'user': 'root',
    'password': '1234567',
    'host': 'localhost',
    'port': 3306,
    'database': 'web_scraping'
}


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


def scrape_and_insert_data():
    driver = init_webdriver()
    url = "https://www.linkedin.com/jobs/search/?currentJobId=4008111986&distance=25.0&f_TPR=r86400&f_WT=2&geoId=92000000&keywords=php&origin=JOB_SEARCH_PAGE_JOB_FILTER"
    driver.get(url)
    driver.implicitly_wait(10)
    driver.execute_script("window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });")
    time.sleep(3)
    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "html.parser")
    section = soup.find("section", class_="two-pane-serp-page__results-list")
    if section is None:
        print("Section not found. The structure might have changed.")
        return

    job_sections = section.find_all("div",
                                    class_="base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card")

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO linkedin (job_title, job_link, company_name, company_link, job_source, job_location, salary, job_type, job_description, job_posted_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for job_section in job_sections:
        job_title = (job_section.find("h3", class_="base-search-card__title").text.strip()) if job_section.find("h3",
                                                                                                                class_="base-search-card__title") else "N/A"
        job_link_tag = job_section.find("a",
                                        class_="base-card__full-link absolute top-0 right-0 bottom-0 left-0 p-0 z-[2]")
        job_link = job_link_tag['href'].strip() if job_link_tag else "N/A"
        job_description = (job_section.find("div",
                                            class_="show-more-less-html__markup relative overflow-hidden").text.strip()) if job_section.find(
            "div", class_="show-more-less-html__markup relative overflow-hidden") else "N/A"
        company_name = (job_section.find("a", class_="hidden-nested-link").text.strip()) if job_section.find("a",
                                                                                                             class_="hidden-nested-link") else "N/A"
        company_link_tag = job_section.find("a", class_="hidden-nested-link")
        company_link = company_link_tag['href'].strip() if company_link_tag else "N/A"
        job_source = "LinkedIn"
        job_type = (job_section.find("li",
                                     class_="job-details-jobs-unified-top-card__job-insight job-details-jobs-unified-top-card__job-insight--highlight").text.strip()) if job_section.find(
            "li",
            class_="job-details-jobs-unified-top-card__job-insight job-details-jobs-unified-top-card__job-insight--highlight") else "N/A"
        job_location = (job_section.find("span", class_="job-search-card__location").text.strip()) if job_section.find(
            "span", class_="job-search-card__location") else "N/A"
        salary = (job_section.find("div", class_="salary compensation__salary").text.strip()) if job_section.find("div",
                                                                                                                  class_="salary compensation__salary") else "N/A"
        job_posted_date = (
            job_section.find("time", class_="job-search-card__listdate--new").text.strip()) if job_section.find("time",
                                                                                                                class_="job-search-card__listdate--new") else "N/A"

        data = (
        job_title, job_link, company_name, company_link, job_source, job_location, salary, job_type, job_description,
        job_posted_date)
        try:
            cursor.execute(insert_query, data)
        except mysql.connector.Error as err:
            print(f"Error inserting data: {err}")
            print(f"Failed data: {data}")

    conn.commit()
    cursor.close()
    conn.close()


@app.route('/scrape', methods=['POST'])
def scrape():
    scrape_and_insert_data()
    return jsonify({"status": "success", "message": "Data scraped and inserted successfully."})


@app.route('/api/jobs', methods=['POST'])
def get_jobs():
    # Retrieve the search query and pagination parameters
    data = request.get_json() or {}
    search_query = data.get('search', '')
    page = data.get('page', 1)
    per_page = data.get('limit', 10)

    page = int(page)
    per_page = int(per_page)
    offset = (page - 1) * per_page

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    search_query = f"%{search_query}%"

    # Paginated request
    page = int(page)
    offset = (page - 1) * per_page  # Calculate the offset

    query = """
                SELECT * FROM linkedin
                WHERE job_title LIKE %s OR job_link LIKE %s OR company_name LIKE %s OR company_link LIKE %s OR job_source LIKE %s OR job_location LIKE %s OR salary LIKE %s OR job_type LIKE %s OR job_description LIKE %s OR job_posted_date LIKE %s
                LIMIT %s OFFSET %s
            """
    cursor.execute(query, (
        search_query, search_query, search_query, search_query, search_query, search_query, search_query, search_query,
        search_query, search_query, per_page, offset))

    jobs = cursor.fetchall()

    # Get the total number of jobs for pagination metadata
    cursor.execute(
        "SELECT COUNT(*) as total FROM linkedin WHERE job_title LIKE %s OR job_link LIKE %s OR company_name LIKE %s OR company_link LIKE %s OR job_source LIKE %s OR job_location LIKE %s OR salary LIKE %s OR job_type LIKE %s OR job_description LIKE %s OR job_posted_date LIKE %s",
        (search_query, search_query, search_query, search_query, search_query, search_query, search_query,
         search_query, search_query, search_query))
    total_jobs = cursor.fetchone()['total']

    # Return jobs along with pagination metadata
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
    app.run(port=5000)
