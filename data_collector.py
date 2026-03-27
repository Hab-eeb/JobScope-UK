"""
JobScope UK — Data Collector
Pulls job listings from the Adzuna API for UK data & AI roles.

Usage:
    python data_collector.py              # Collect all role types
    python data_collector.py --roles "data scientist" "ml engineer"  # Specific roles only
    python data_collector.py --max-pages 3  # Limit pages per role (default: 5)
"""

import os
import sys
import json
import time
import sqlite3
import argparse
import requests
from datetime import datetime
from dotenv import load_dotenv

from database import init_db, get_raw_job_count, get_raw_job_count_by_search_term

load_dotenv()
# Adzuna API 
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
DB_NAME = os.getenv("DB_NAME", "jobscope.db")
# Reed API
REED_API_KEY = os.getenv("REED_API_KEY")


REED_BASE_URL = "https://www.reed.co.uk/api/1.0/search"
ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs/gb/search"

# Role types to search for
DEFAULT_SEARCH_TERMS = [
    "data scientist",
    "data analyst",
    "machine learning engineer",
    "data engineer",
    "AI engineer",
    "NLP engineer",
    "LLM engineer",
    "business intelligence analyst",
]


def fetch_adzuna_jobs_page(search_term: str, page: int = 1, results_per_page: int = 50) -> dict:
    """Fetches a single page of job results from the Adzuna API."""

    url = f"{ADZUNA_BASE_URL}/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": results_per_page,
        "what": search_term,
        "content-type": "application/json",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def save_adzuna_jobs_to_db(jobs: list, search_term: str) -> int:
    """Saves a list of Adzuna job results to the raw_jobs table. 
    Returns the number of new jobs inserted (skips duplicates).
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    new_count = 0
    date_collected = datetime.now().isoformat()

    for job in jobs:
        # Extract salary info
        salary_min = job.get("salary_min")
        salary_max = job.get("salary_max")
        salary_is_predicted = job.get("salary_is_predicted", 0)

        # Extract location
        location_parts = job.get("location", {}).get("display_name", "")

        # Extract category
        category = job.get("category", {}).get("label", "")

        try:
            cursor.execute('''
                INSERT INTO raw_jobs 
                (source, external_id, title, company, description, location,
                 salary_min, salary_max, salary_is_predicted, category, url,
                 date_posted, date_collected, search_term)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                "adzuna",
                str(job.get("id", "")),
                job.get("title", ""),
                job.get("company", {}).get("display_name", ""),
                job.get("description", ""),
                location_parts,
                salary_min,
                salary_max,
                1 if salary_is_predicted else 0,
                category,
                job.get("redirect_url", ""),
                job.get("created", ""),
                date_collected,
                search_term,
            ))
            new_count += 1
        except sqlite3.IntegrityError:
            # Duplicate — skip (UNIQUE constraint on source + external_id)
            pass

    conn.commit()
    conn.close()
    return new_count


def adzuna_collect_for_term(search_term: str, max_pages: int = 5) -> int:
    """Collects jobs for a single search term across multiple pages.
    Returns total new jobs inserted.
    """
    total_new = 0
    print(f"\n  [Adzuna] Searching: '{search_term}'")

    for page in range(1, max_pages + 1):
        try:
            data = fetch_adzuna_jobs_page(search_term, page=page)
            results = data.get("results", [])

            if not results:
                print(f"    Page {page}: No more results.")
                break

            new = save_adzuna_jobs_to_db(results, search_term)
            total_new += new
            total_on_page = len(results)
            print(f"    Page {page}: {total_on_page} fetched, {new} new saved")

            # Respect rate limits — small delay between pages
            time.sleep(0.5)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"    Rate limited. Waiting 60s...")
                time.sleep(60)
                # Retry this page
                try:
                    data = fetch_adzuna_jobs_page(search_term, page=page)
                    results = data.get("results", [])
                    new = save_adzuna_jobs_to_db(results, search_term)
                    total_new += new
                except Exception as retry_err:
                    print(f"    Retry failed: {retry_err}")
            else:
                print(f"    HTTP Error on page {page}: {e}")
                break
        except Exception as e:
            print(f"    Error on page {page}: {e}")
            break

    return total_new


def fetch_reed_jobs_page(search_term: str, skip: int = 0, results_to_take: int = 100) -> list:
    """Fetches a page of job results from the Reed API."""

    params = {
        "keywords": search_term,
        "resultsToTake": results_to_take,
        "resultsToSkip": skip,
    }

    response = requests.get(
        REED_BASE_URL,
        params=params,
        auth=(REED_API_KEY, ""),  # Basic auth: API key as username, empty password
    )
    response.raise_for_status()
    return response.json().get("results", [])

def fetch_reed_job_details(job_id: str) -> dict:
    """Fetches full job details from Reed API."""
    url = f"https://www.reed.co.uk/api/1.0/jobs/{job_id}"
    response = requests.get(url, auth=(REED_API_KEY, ""))
    response.raise_for_status()
    return response.json()


def save_reed_jobs_to_db(jobs: list, search_term: str) -> int:
    """Saves Reed job results to the raw_jobs table. Returns new jobs inserted."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    new_count = 0
    date_collected = datetime.now().isoformat()

    for job in jobs:
        try:
            cursor.execute('''
                INSERT INTO raw_jobs
                (source, external_id, title, company, description, location,
                 salary_min, salary_max, salary_is_predicted, category, url,
                 date_posted, date_collected, search_term)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                "reed",
                str(job.get("jobId", "")),
                job.get("jobTitle", ""),
                job.get("employerName", ""),
                job.get("jobDescription", ""),
                job.get("locationName", ""),
                job.get("minimumSalary"),
                job.get("maximumSalary"),
                0,
                "",
                job.get("jobUrl", ""),
                job.get("date", ""),
                date_collected,
                search_term,
            ))
            new_count += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    return new_count


def collect_reed_for_term(search_term: str, max_pages: int = 3) -> int:
    """Collects Reed jobs with full descriptions for a single search term."""
    total_new = 0
    results_per_page = 100
    print(f"\n  [Reed] Searching: '{search_term}'")

    for page in range(max_pages):
        skip = page * results_per_page
        try:
            results = fetch_reed_jobs_page(search_term, skip=skip, results_to_take=results_per_page)

            if not results:
                print(f"    Page {page+1}: No more results.")
                break

            # Fetch full details for each job
            full_jobs = []
            for job in results:
                job_id = job.get("jobId")
                try:
                    details = fetch_reed_job_details(str(job_id))
                    full_jobs.append(details)
                    time.sleep(0.3)  # Rate limit: ~3 requests/sec
                except Exception as e:
                    # Fall back to search result if details fetch fails
                    full_jobs.append(job)

            new = save_reed_jobs_to_db(full_jobs, search_term)
            total_new += new
            print(f"    Page {page+1}: {len(results)} fetched, {new} new saved (with full descriptions)")

        except requests.exceptions.HTTPError as e:
            print(f"    HTTP Error: {e}")
            break
        except Exception as e:
            print(f"    Error: {e}")
            break

    return total_new

# def save_reed_jobs_to_db(jobs: list, search_term: str) -> int:
#     """Saves Reed job results to the raw_jobs table. Returns new jobs inserted."""
#     conn = sqlite3.connect(DB_NAME)
#     cursor = conn.cursor()
#     new_count = 0
#     date_collected = datetime.now().isoformat()

#     for job in jobs:
#         try:
#             cursor.execute('''
#                 INSERT INTO raw_jobs
#                 (source, external_id, title, company, description, location,
#                  salary_min, salary_max, salary_is_predicted, category, url,
#                  date_posted, date_collected, search_term)
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#             ''', (
#                 "reed",
#                 str(job.get("jobId", "")),
#                 job.get("jobTitle", ""),
#                 job.get("employerName", ""),
#                 job.get("jobDescription", ""),
#                 job.get("locationName", ""),
#                 job.get("minimumSalary"),
#                 job.get("maximumSalary"),
#                 0,  # Reed doesn't flag predicted salaries
#                 "",  # Reed doesn't return categories in search
#                 job.get("jobUrl", ""),
#                 job.get("date", ""),
#                 date_collected,
#                 search_term,
#             ))
#             new_count += 1
#         except sqlite3.IntegrityError:
#             pass

#     conn.commit()
#     conn.close()
#     return new_count


# def collect_reed_for_term(search_term: str, max_pages: int = 3) -> int:
#     """Collects Reed jobs for a single search term. Returns total new jobs."""
#     total_new = 0
#     results_per_page = 100  # Reed allows up to 100
#     print(f"\n  [Reed] Searching: '{search_term}'")

#     for page in range(max_pages):
#         skip = page * results_per_page
#         try:
#             results = fetch_reed_jobs_page(search_term, skip=skip, results_to_take=results_per_page)

#             if not results:
#                 print(f"    Page {page+1}: No more results.")
#                 break

#             new = save_reed_jobs_to_db(results, search_term)
#             total_new += new
#             print(f"    Page {page+1}: {len(results)} fetched, {new} new saved")

#             time.sleep(0.5)

#         except requests.exceptions.HTTPError as e:
#             print(f"    HTTP Error: {e}")
#             break
#         except Exception as e:
#             print(f"    Error: {e}")
#             break

#     return total_new

def run_collection(search_terms: list = None, max_pages: int = 5):
    """Main collection pipeline. Iterates through search terms and collects from Adzuna + Reed."""

    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        print("ERROR: ADZUNA_APP_ID and ADZUNA_APP_KEY must be set in .env")
        sys.exit(1)

    init_db()
    terms = search_terms or DEFAULT_SEARCH_TERMS
    start_count = get_raw_job_count()

    print(f"Starting collection — {len(terms)} search terms")
    print(f"Existing jobs in DB: {start_count}")

    grand_total = 0

    # Adzuna collection
    print(f"\n--- ADZUNA (up to {max_pages} pages each) ---")
    for term in terms:
        new = adzuna_collect_for_term(term, max_pages=max_pages)
        grand_total += new
        time.sleep(1)

    # Reed collection (if API key is set)
    if REED_API_KEY:
        print(f"\n--- REED (up to 3 pages each) ---")
        for term in terms:
            new = collect_reed_for_term(term, max_pages=3)
            grand_total += new
            time.sleep(1)
    else:
        print("\nREED_API_KEY not set — skipping Reed collection.")

    end_count = get_raw_job_count()

    print(f"\n{'='*50}")
    print(f"Collection complete!")
    print(f"New jobs added: {grand_total}")
    print(f"Total jobs in DB: {end_count}")
    print(f"\nBreakdown by search term:")
    for term, count in get_raw_job_count_by_search_term():
        print(f"  {term}: {count}")
    print(f"\nBreakdown by source:")
    conn = sqlite3.connect(DB_NAME)
    for source, count in conn.execute('SELECT source, COUNT(*) FROM raw_jobs GROUP BY source').fetchall():
        print(f"  {source}: {count}")
    conn.close()
    print(f"{'='*50}")

 
def main():
    parser = argparse.ArgumentParser(description="JobScope UK — Data Collector")
    parser.add_argument(
        "--roles", nargs="+", default=None,
        help="Specific role types to search (default: all 8 role types)"
    )
    parser.add_argument(
        "--max-pages", type=int, default=5,
        help="Max pages to fetch per role type (default: 5, ~50 jobs/page)"
    )

    args = parser.parse_args()
    run_collection(search_terms=args.roles, max_pages=args.max_pages)

if __name__ == "__main__":
    main()