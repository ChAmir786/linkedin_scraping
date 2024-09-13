import time
import psycopg2
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


def init():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    return webdriver.Chrome(options=chrome_options)


def crawlPage(driver):
    url = "https://www.linkedin.com/jobs/search?keywords=&location=United%20States&geoId=103644278&f_TPR=&f_WT=2&position=1&pageNum=0"
    driver.get(url)
    driver.implicitly_wait(10)
    driver.execute_script("window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });")
    time.sleep(3)
    return driver.page_source


def parseHtml(html):
    return BeautifulSoup(html, "html.parser")


def findSections(soup):
    section = soup.find("section", class_="two-pane-serp-page__results-list")
    if section is None:
        print("Section not found. The structure might have changed.")
    return section


def findjobs(p_section):
    return p_section.find_all("div",
                              class_="base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card")


def extractJobData(job_sections):
    job_title = (job_sections.find("h3", class_="base-search-card__title")
                 .text.strip()) if job_sections.find("h3", class_="base-search-card__title") else "N/A"
    job_link_tag = job_sections.find("a",
                                     class_="base-card__full-link absolute top-0 right-0 bottom-0 left-0 p-0 z-[2]")
    job_link = job_link_tag['href'].strip() if job_link_tag else "N/A"

    job_description = (job_sections.find("div", class_="show-more-less-html__markup relative overflow-hidden")
                       .text.strip()) if job_sections.find("div",
                                                           class_="show-more-less-html__markup relative overflow-hidden") else "N/A"
    company_name = (job_sections.find("a", class_="hidden-nested-link")
                    .text.strip()) if job_sections.find("a", class_="hidden-nested-link") else "N/A"
    company_link_tag = job_sections.find("a", class_="hidden-nested-link")
    company_link = company_link_tag['href'].strip() if company_link_tag else "N/A"

    job_source = "LinkedIn"
    job_type = (job_sections.find("li",
                                  class_="job-details-jobs-unified-top-card__job-insight job-details-jobs-unified-top-card__job-insight--highlight")
                .text.strip()) if job_sections.find("li",
                                                    class_="job-details-jobs-unified-top-card__job-insight job-details-jobs-unified-top-card__job-insight--highlight") else "N/A"
    job_location = (job_sections.find("span", class_="job-search-card__location")
                    .text.strip()) if job_sections.find("span", class_="job-search-card__location") else "N/A"
    salary = (job_sections.find("div", class_="salary compensation__salary")
              .text.strip()) if job_sections.find("div", class_="salary compensation__salary") else "N/A"
    job_posted_date = (job_sections.find("time", class_="job-search-card__listdate--new")
                       .text.strip()) if job_sections.find("time", class_="job-search-card__listdate--new") else "N/A"

    return {
        "JobTitle": job_title,
        "JobLink": job_link,
        "CompanyName": company_name,
        "CompanyLink": company_link,
        "JobSource": job_source,
        "JobLocation": job_location,
        "Salary": salary,
        "JobType": job_type,
        "JobDescription": job_description,
        "JobPostedDate": job_posted_date
    }


def insert_job_data(cursor, data):
    try:
        check_query = """
        SELECT 1 FROM linkedin WHERE job_title = %s LIMIT 1
        """
        cursor.execute(check_query, (data['JobTitle'],))
        result = cursor.fetchone()

        # If result is None, the job does not exist, proceed to insert it
        if result is None:
            insert_query = """
            INSERT INTO linkedin (job_title, job_link, company_name, company_link, job_source, job_location, salary, job_type, job_description, job_posted_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                data['JobTitle'],
                data['JobLink'],
                data['CompanyName'],
                data['CompanyLink'],
                data['JobSource'],
                data['JobLocation'],
                data['Salary'],
                data['JobType'],
                data['JobDescription'],
                data['JobPostedDate']
            ))
        else:
            print(f"Job with title '{data['JobTitle']}' already exists in the database.")

    except psycopg2.Error as err:
        print(f"Error inserting data: {err}")
        print(f"Failed data: {data}")


def main():
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
        html = crawlPage(driver)
        soup = parseHtml(html)
        # print(soup.prettify())
        p_section = findSections(soup)
        print("Total Containers: " + str(len(p_section)))
        job_sections = findjobs(p_section)
        # print(job_sections)

        # Connect to the database
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()

        for job_section in job_sections:
            data = extractJobData(job_section)
            insert_job_data(cursor, data)

        # Commit the transaction
        conn.commit()

    except psycopg2.Error as err:
        print(f"Database Error: {err}")

    finally:
        # Close the cursor and connection if they were initialized
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
        driver.quit()


if __name__ == "__main__":
    main()
