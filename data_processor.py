"""
JobScope UK — Data Processor
Cleans raw job data and extracts structured information:
- HTML stripping
- Title normalisation and role categorisation
- Seniority inference
- Location parsing
- Salary midpoint calculation
- Skill extraction using the curated taxonomy

Usage:
    python data_processor.py          # Process all unprocessed raw jobs
    python data_processor.py --reset  # Clear clean_jobs and reprocess everything
"""

import os
import re
import json
import sqlite3
import argparse
from html import unescape
from dotenv import load_dotenv

from database import init_db, get_clean_job_count
from skill_taxonomy import ALL_SKILLS, SKILL_TO_CATEGORY

load_dotenv()
DB_NAME = os.getenv("DB_NAME", "jobscope.db")


# ── Title Normalisation ──────────────────────────────────────────────

# Maps keyword patterns in job titles to standardised role categories
ROLE_CATEGORY_RULES = [
    # Order matters — more specific matches first
    (r"\b(nlp|natural language processing)\b", "NLP Engineer"),
    (r"\b(llm|large language model)\b", "LLM Engineer"),
    (r"\b(ml|machine learning)\s*(engineer|developer|ops)\b", "ML Engineer"),
    (r"\b(ai|artificial intelligence)\s*(engineer|developer)\b", "AI Engineer"),
    (r"\bdata\s*scientist\b", "Data Scientist"),
    (r"\bdata\s*engineer\b", "Data Engineer"),
    (r"\bdata\s*analy", "Data Analyst"),  # matches analyst, analytics
    (r"\b(bi|business intelligence)\b", "BI Analyst"),
    (r"\banalytics\s*engineer\b", "Data Engineer"),
    (r"\bresearch\s*(scientist|engineer)\b", "Data Scientist"),
    (r"\bml\b", "ML Engineer"),
    (r"\b(ai|artificial intelligence)\b", "AI Engineer"),
]


def categorise_role(title: str) -> str:
    """Maps a job title to a standardised role category."""
    title_lower = title.lower()
    for pattern, category in ROLE_CATEGORY_RULES:
        if re.search(pattern, title_lower):
            return category
    return "Other"


def normalise_title(title: str) -> str:
    """Cleans up a job title — strips extra whitespace, removes junk characters."""
    # Remove HTML entities
    title = unescape(title)
    # Remove common junk patterns
    title = re.sub(r'\s*[\-–|/].*$', '', title)  # Remove everything after dash/pipe
    title = re.sub(r'\(.*?\)', '', title)  # Remove parenthetical content
    title = re.sub(r'\s+', ' ', title).strip()  # Collapse whitespace
    return title.title()  # Title Case


# ── Seniority Inference ───────────────────────────────────────────────

SENIORITY_RULES = {
    "junior": [r"\bjunior\b", r"\bjnr\b", r"\bgraduate\b", r"\bgrad\b",
               r"\bentry\s*level\b", r"\bintern\b", r"\btrainee\b",
               r"\bassociate\b"],
    "senior": [r"\bsenior\b", r"\bsnr\b", r"\bsr\b", r"\blead\b",
               r"\bprincipal\b", r"\bstaff\b", r"\bhead\s*of\b",
               r"\bdirector\b", r"\bmanager\b", r"\bvp\b",
               r"\bchief\b"],
}


def infer_seniority(title: str) -> str:
    """Infers seniority from job title keywords."""
    title_lower = title.lower()
    for level, patterns in SENIORITY_RULES.items():
        for pattern in patterns:
            if re.search(pattern, title_lower):
                return level
    return "mid"


# ── Location Parsing ──────────────────────────────────────────────────

# Major UK regions for normalisation
UK_REGIONS = {
    "london": "London",
    "greater london": "London",
    "city of london": "London",
    "manchester": "Greater Manchester",
    "greater manchester": "Greater Manchester",
    "birmingham": "West Midlands",
    "west midlands": "West Midlands",
    "leeds": "West Yorkshire",
    "west yorkshire": "West Yorkshire",
    "bristol": "Bristol",
    "edinburgh": "Scotland",
    "glasgow": "Scotland",
    "scotland": "Scotland",
    "cardiff": "Wales",
    "wales": "Wales",
    "cambridge": "East of England",
    "oxford": "South East",
    "reading": "South East",
    "south east": "South East",
    "south west": "South West",
    "north west": "North West",
    "north east": "North East",
    "east midlands": "East Midlands",
    "yorkshire": "Yorkshire",
    "belfast": "Northern Ireland",
    "northern ireland": "Northern Ireland",
    "remote": "Remote",
}


def parse_location(location_str: str) -> tuple:
    """Parses a location string into (city, region). Returns best match."""
    if not location_str:
        return ("Unknown", "Unknown")

    loc_lower = location_str.lower().strip()

    # Check for remote
    if "remote" in loc_lower or "home" in loc_lower or "anywhere" in loc_lower:
        return ("Remote", "Remote")

    # Try to match known regions/cities
    city = location_str.split(",")[0].strip() if "," in location_str else location_str.strip()

    region = "Other UK"
    for keyword, mapped_region in UK_REGIONS.items():
        if keyword in loc_lower:
            region = mapped_region
            break

    return (city, region)


# ── Text Cleaning ─────────────────────────────────────────────────────

def clean_description(raw_html: str) -> str:
    """Strips HTML tags and normalises whitespace from job descriptions."""
    if not raw_html:
        return ""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', raw_html)
    # Decode HTML entities
    text = unescape(text)
    # Remove URLs
    text = re.sub(r'https?://\S+', '', text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text.lower()


# ── Skill Extraction ─────────────────────────────────────────────────

# Pre-compile regex patterns for performance
_SKILL_PATTERNS = {}
for skill in ALL_SKILLS:
    # Handle special cases for short terms that could match inside words
    if len(skill) <= 2:
        # Very short skills (R, AI, BI) — require word boundaries and context
        _SKILL_PATTERNS[skill] = re.compile(
            r'(?<![a-zA-Z])' + re.escape(skill) + r'(?![a-zA-Z])',
            re.IGNORECASE
        )
    else:
        _SKILL_PATTERNS[skill] = re.compile(
            r'\b' + re.escape(skill) + r'\b',
            re.IGNORECASE
        )


def extract_skills(description_clean: str) -> list:
    """Extracts matching skills from a cleaned job description."""
    found = []
    for skill, pattern in _SKILL_PATTERNS.items():
        if pattern.search(description_clean):
            found.append(skill)
    return sorted(set(found))


# ── Main Processing Pipeline ─────────────────────────────────────────

def process_all_jobs(reset: bool = False):
    """Processes all raw jobs into the clean_jobs table."""

    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row

    if reset:
        conn.execute("DELETE FROM clean_jobs")
        conn.commit()
        print("Cleared clean_jobs table.")

    # Fetch raw jobs that haven't been processed yet
    if reset:
        raw_jobs = conn.execute("SELECT * FROM raw_jobs").fetchall()
    else:
        raw_jobs = conn.execute("""
            SELECT r.* FROM raw_jobs r
            LEFT JOIN clean_jobs c ON c.raw_job_id = r.id
            WHERE c.id IS NULL
        """).fetchall()

    total = len(raw_jobs)
    if total == 0:
        print("No new jobs to process.")
        conn.close()
        return

    print(f"Processing {total} raw jobs...")

    processed = 0
    skipped = 0

    for job in raw_jobs:
        title_original = job["title"] or ""
        title_normalized = normalise_title(title_original)
        role_category = categorise_role(title_original)
        seniority = infer_seniority(title_original)

        # Skip jobs that don't match any data/AI role
        if role_category == "Other":
            skipped += 1
            continue

        description_clean = clean_description(job["description"])
        city, region = parse_location(job["location"])

        salary_min = job["salary_min"]
        salary_max = job["salary_max"]
        salary_mid = None
        if salary_min and salary_max:
            salary_mid = (salary_min + salary_max) / 2

        has_real_salary = 0 if job["salary_is_predicted"] else 1
        if not salary_min and not salary_max:
            has_real_salary = 0

        skills = extract_skills(description_clean)

        try:
            conn.execute('''
                INSERT INTO clean_jobs
                (raw_job_id, title_original, title_normalized, role_category, company,
                 description_clean, location_raw, location_city, location_region,
                 salary_min, salary_max, salary_mid, has_real_salary,
                 extracted_skills, date_posted, seniority)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job["id"],
                title_original,
                title_normalized,
                role_category,
                job["company"] or "",
                description_clean,
                job["location"] or "",
                city,
                region,
                salary_min,
                salary_max,
                salary_mid,
                has_real_salary,
                json.dumps(skills),
                job["date_posted"] or "",
                seniority,
            ))
            processed += 1
        except sqlite3.IntegrityError:
            # Already processed this raw_job_id
            pass

        if processed % 200 == 0 and processed > 0:
            conn.commit()
            print(f"  ...processed {processed}/{total}")

    conn.commit()
    conn.close()

    print(f"\nProcessing complete!")
    print(f"  Processed: {processed}")
    print(f"  Skipped (non-matching roles): {skipped}")
    print(f"  Total clean jobs in DB: {get_clean_job_count()}")

    # Print summary stats
    print_summary()


def print_summary():
    """Prints a summary of the clean_jobs data."""
    conn = sqlite3.connect(DB_NAME)

    print(f"\n{'='*50}")
    print("CLEAN DATA SUMMARY")
    print(f"{'='*50}")

    # By role category
    print("\nJobs by role category:")
    rows = conn.execute(
        "SELECT role_category, COUNT(*) as cnt FROM clean_jobs GROUP BY role_category ORDER BY cnt DESC"
    ).fetchall()
    for role, count in rows:
        print(f"  {role}: {count}")

    # By seniority
    print("\nJobs by seniority:")
    rows = conn.execute(
        "SELECT seniority, COUNT(*) as cnt FROM clean_jobs GROUP BY seniority ORDER BY cnt DESC"
    ).fetchall()
    for level, count in rows:
        print(f"  {level}: {count}")

    # By region (top 10)
    print("\nTop 10 regions:")
    rows = conn.execute(
        "SELECT location_region, COUNT(*) as cnt FROM clean_jobs GROUP BY location_region ORDER BY cnt DESC LIMIT 10"
    ).fetchall()
    for region, count in rows:
        print(f"  {region}: {count}")

    # Salary coverage
    total = conn.execute("SELECT COUNT(*) FROM clean_jobs").fetchone()[0]
    with_salary = conn.execute("SELECT COUNT(*) FROM clean_jobs WHERE salary_mid IS NOT NULL AND has_real_salary = 1").fetchone()[0]
    print(f"\nSalary data: {with_salary}/{total} jobs ({100*with_salary/total:.0f}%) have real salary info")

    # Top 15 skills
    print("\nTop 15 extracted skills:")
    rows = conn.execute("SELECT extracted_skills FROM clean_jobs").fetchall()
    skill_counts = {}
    for row in rows:
        skills = json.loads(row[0])
        for s in skills:
            skill_counts[s] = skill_counts.get(s, 0) + 1
    top_skills = sorted(skill_counts.items(), key=lambda x: x[1], reverse=True)[:15]
    for skill, count in top_skills:
        pct = 100 * count / total
        print(f"  {skill}: {count} ({pct:.0f}%)")

    print(f"{'='*50}")
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="JobScope UK — Data Processor")
    parser.add_argument("--reset", action="store_true",
                    help="Clear clean_jobs and reprocess everything")
    args = parser.parse_args()
    process_all_jobs(reset=args.reset)



if __name__ == "__main__":
    main()
    
