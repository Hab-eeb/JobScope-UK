"""
JobScope UK — Reed Full Description Fetcher
Fetches full job descriptions from the Reed job details API
and updates the raw_jobs table.

Reed's search endpoint returns truncated descriptions (~453 chars).
The details endpoint returns the full text.

Usage:
    python fetch_full_descriptions.py              # Fetch all unfetched Reed jobs
    python fetch_full_descriptions.py --limit 500  # Fetch up to 500 (for rate limit management)

Note: Reed allows ~1000 requests/day. Run with --limit 950 on day 1,
then run again without --limit on day 2 to finish.
"""

import os
import sys
import time
import sqlite3
import argparse
import requests
from dotenv import load_dotenv

load_dotenv()

REED_API_KEY = os.getenv("REED_API_KEY")
DB_NAME = os.getenv("DB_NAME", "jobscope.db")
REED_DETAILS_URL = "https://www.reed.co.uk/api/1.0/jobs"


def fetch_job_details(job_id: str) -> dict:
    """Fetches full job details from Reed API."""
    url = f"{REED_DETAILS_URL}/{job_id}"
    response = requests.get(url, auth=(REED_API_KEY, ""))
    response.raise_for_status()
    return response.json()


def get_unfetched_reed_jobs(limit: int = None) -> list:
    """Returns Reed jobs whose descriptions are still truncated (< 500 chars)."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT id, external_id, LENGTH(description) as desc_len
        FROM raw_jobs 
        WHERE source = 'reed' AND LENGTH(description) <= 500
        ORDER BY id
    """
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(query).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_description(db_id: int, full_description: str):
    """Updates a raw_job's description with the full text."""
    conn = sqlite3.connect(DB_NAME)
    conn.execute(
        "UPDATE raw_jobs SET description = ? WHERE id = ?",
        (full_description, db_id)
    )
    conn.commit()
    conn.close()


def run_fetch(limit: int = None):
    """Main fetch loop."""
    if not REED_API_KEY:
        print("ERROR: REED_API_KEY not set in .env")
        sys.exit(1)

    jobs = get_unfetched_reed_jobs(limit)
    total = len(jobs)

    if total == 0:
        print("All Reed jobs already have full descriptions.")
        return

    print(f"Fetching full descriptions for {total} Reed jobs...")

    success = 0
    failed = 0

    for i, job in enumerate(jobs):
        try:
            details = fetch_job_details(job["external_id"])
            full_desc = details.get("jobDescription", "")

            if full_desc and len(full_desc) > len(""):
                update_description(job["id"], full_desc)
                success += 1
            else:
                failed += 1

            if (i + 1) % 50 == 0:
                print(f"  Progress: {i+1}/{total} ({success} updated, {failed} failed)")

            # Rate limit: ~2 requests/second to stay well within limits
            time.sleep(0.5)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"\n  Rate limited at job {i+1}. Stopping. Run again tomorrow to continue.")
                break
            else:
                failed += 1
                print(f"  Error fetching job {job['external_id']}: {e}")
        except Exception as e:
            failed += 1
            print(f"  Error: {e}")

    # Print results
    print(f"\nFetch complete!")
    print(f"  Updated: {success}")
    print(f"  Failed: {failed}")
    print(f"  Remaining: {total - success - failed}")

    # Show new description length stats
    conn = sqlite3.connect(DB_NAME)
    row = conn.execute(
        "SELECT AVG(LENGTH(description)), MAX(LENGTH(description)) FROM raw_jobs WHERE source = 'reed'"
    ).fetchone()
    print(f"\nReed descriptions now: avg={row[0]:.0f} chars, max={row[1]} chars")
    conn.close()

    if total - success - failed > 0:
        print("\nSome jobs remaining. Run again tomorrow to fetch the rest (rate limit).")
    else:
        print("\nAll done! Run 'python data_processor.py --reset' to reprocess with full descriptions.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch full Reed job descriptions")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max jobs to fetch (default: all unfetched)")
    args = parser.parse_args()
    run_fetch(limit=args.limit)
