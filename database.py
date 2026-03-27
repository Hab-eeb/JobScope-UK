import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()
DB_NAME = os.getenv("DB_NAME", "jobscope.db")


def init_db():
    """Creates the raw_jobs and clean_jobs tables."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Raw data as collected from APIs
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            external_id TEXT,
            title TEXT,
            company TEXT,
            description TEXT,
            location TEXT,
            salary_min REAL,
            salary_max REAL,
            salary_is_predicted INTEGER,
            category TEXT,
            url TEXT,
            date_posted TEXT,
            date_collected TEXT,
            search_term TEXT,
            UNIQUE(source, external_id)
        )
    ''')

    # Cleaned + enriched data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clean_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_job_id INTEGER REFERENCES raw_jobs(id),
            job_source TEXT,
            title_original TEXT,
            title_normalized TEXT,
            role_category TEXT,
            company TEXT,
            description_clean TEXT,
            location_raw TEXT,
            location_city TEXT,
            location_region TEXT,
            salary_min REAL,
            salary_max REAL,
            salary_mid REAL,
            has_real_salary INTEGER,
            extracted_skills TEXT,
            date_posted TEXT,
            seniority TEXT,
            UNIQUE(raw_job_id)
        )
    ''')

    conn.commit()
    conn.close()
    print(f"Database initialized: {DB_NAME}")


def get_raw_job_count():
    """Returns the total number of raw jobs collected."""
    conn = sqlite3.connect(DB_NAME)
    count = conn.execute('SELECT COUNT(*) FROM raw_jobs').fetchone()[0]
    conn.close()
    return count


def get_raw_job_count_by_search_term():
    """Returns job counts grouped by search term."""
    conn = sqlite3.connect(DB_NAME)
    rows = conn.execute(
        'SELECT search_term, COUNT(*) as cnt FROM raw_jobs GROUP BY search_term ORDER BY cnt DESC'
    ).fetchall()
    conn.close()
    return rows


def get_clean_job_count():
    """Returns the total number of cleaned jobs."""
    conn = sqlite3.connect(DB_NAME)
    count = conn.execute('SELECT COUNT(*) FROM clean_jobs').fetchone()[0]
    conn.close()
    return count


if __name__ == "__main__":
    init_db()
