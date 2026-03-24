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
    (r"\bdata\s*scien", "Data Scientist"),  # matches scientist, science
    (r"\bdata\s*engineer\b", "Data Engineer"),
    (r"\bdata\s*platform\b", "Data Engineer"),
    (r"\bdata\s*analy", "Data Analyst"),  # matches analyst, analytics, analysis
    (r"\bdata\s*product\b", "Data Scientist"),
    (r"\bdata\s*manage", "Data Analyst"),  # data management roles
    (r"\bdata\s*business\s*analyst\b", "Data Analyst"),
    (r"\b(bi|business intelligence)\b", "BI Analyst"),
    (r"\bbusiness\s*analyst\b", "BI Analyst"),
    (r"\banalytics\s*engineer\b", "Data Engineer"),
    (r"\bresearch\s*(scientist|engineer)\b", "Data Scientist"),
    (r"\bml\b", "ML Engineer"),
    (r"\bdata\s*architect", "Data Engineer"),  # data architecture roles
    (r"\bfinance\s*analyst\b", "Data Analyst"),
    (r"\b(ai|artificial intelligence)\b", "AI Engineer"),
    # Catch analyst variants
    (r"\bproduct\s*analyst\b", "Data Analyst"),
    (r"\bpricing\s*analyst\b", "Data Analyst"),
    (r"\bcommercial\s*analyst\b", "Data Analyst"),
    (r"\bdigital\s*analyst\b", "Data Analyst"),
    (r"\blead\s*analyst\b", "Data Analyst"),
    (r"\bprocurement\s*analyst\b", "Data Analyst"),
    (r"\bfinancial?\s*analyst\b", "Data Analyst"),
    (r"\bfp&a\s*analyst\b", "Data Analyst"),
    # Catch head/director of data
    (r"\bhead\s*of\s*data\b", "Data Scientist"),
    (r"\bdirector.*data\b", "Data Scientist"),
    # Trainee/graduate data roles
    (r"\b(trainee|graduate).*data\b", "Data Analyst"),
    (r"\bdata.*(trainee|graduate)\b", "Data Analyst"),
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
    # London
    "london": "London",
    "greater london": "London",
    "city of london": "London",
    # North West
    "manchester": "North West",
    "greater manchester": "North West",
    "liverpool": "North West",
    "warrington": "North West",
    "blackpool": "North West",
    "lancashire": "North West",
    "cheshire": "North West",
    "north west": "North West",
    # North East
    "newcastle": "North East",
    "tyne": "North East",
    "sunderland": "North East",
    "durham": "North East",
    "north east": "North East",
    # Yorkshire
    "leeds": "Yorkshire",
    "west yorkshire": "Yorkshire",
    "sheffield": "Yorkshire",
    "york": "Yorkshire",
    "bradford": "Yorkshire",
    "yorkshire": "Yorkshire",
    # West Midlands
    "birmingham": "West Midlands",
    "west midlands": "West Midlands",
    "coventry": "West Midlands",
    "telford": "West Midlands",
    "shropshire": "West Midlands",
    # East Midlands
    "nottingham": "East Midlands",
    "derby": "East Midlands",
    "leicester": "East Midlands",
    "east midlands": "East Midlands",
    # East of England
    "cambridge": "East of England",
    "norwich": "East of England",
    "eastern england": "East of England",
    "hertfordshire": "East of England",
    "stevenage": "East of England",
    "essex": "East of England",
    # South East
    "oxford": "South East",
    "reading": "South East",
    "milton keynes": "South East",
    "buckinghamshire": "South East",
    "hampshire": "South East",
    "southampton": "South East",
    "brighton": "South East",
    "surrey": "South East",
    "kent": "South East",
    "south east": "South East",
    # South West
    "bristol": "South West",
    "exeter": "South West",
    "devon": "South West",
    "gloucester": "South West",
    "gloucestershire": "South West",
    "cheltenham": "South West",
    "bath": "South West",
    "south west": "South West",
    # Scotland
    "edinburgh": "Scotland",
    "glasgow": "Scotland",
    "scotland": "Scotland",
    # Wales
    "cardiff": "Wales",
    "swansea": "Wales",
    "wales": "Wales",
    # Northern Ireland
    "belfast": "Northern Ireland",
    "northern ireland": "Northern Ireland",
    # Remote
    "remote": "Remote",
}

# UK postcode area to region mapping (first 1-2 letters of postcode)
POSTCODE_REGIONS = {
    "e": "London", "ec": "London", "n": "London", "nw": "London",
    "se": "London", "sw": "London", "w": "London", "wc": "London",
    "m": "North West", "ol": "North West", "wa": "North West",
    "bl": "North West", "pr": "North West", "l": "North West",
    "ne": "North East", "sr": "North East", "dh": "North East",
    "ls": "Yorkshire", "bd": "Yorkshire", "s": "Yorkshire",
    "b": "West Midlands", "tf": "West Midlands", "cv": "West Midlands",
    "ng": "East Midlands", "de": "East Midlands", "le": "East Midlands",
    "cb": "East of England", "sg": "East of England", "lu": "East of England",
    "ox": "South East", "rg": "South East", "mk": "South East",
    "so": "South East", "bn": "South East",
    "bs": "South West", "gl": "South West", "ex": "South West",
    "pl": "South West",
    "eh": "Scotland", "g": "Scotland",
    "cf": "Wales",
    "bt": "Northern Ireland",
    "cr": "London",  # Croydon
}


def parse_location(location_str: str) -> tuple:
    """Parses a location string into (city, region). Returns best match."""
    if not location_str:
        return ("Unknown", "Unknown")

    loc_lower = location_str.lower().strip()

    # Check for remote
    if "remote" in loc_lower or "home" in loc_lower or "anywhere" in loc_lower:
        return ("Remote", "Remote")

    # Handle generic UK entries
    if loc_lower in ("uk", "united kingdom", "england", "great britain", "gb"):
        return ("UK-wide", "UK-wide")

    # Try to match known regions/cities by keyword
    city = location_str.split(",")[0].strip() if "," in location_str else location_str.strip()

    region = None
    for keyword, mapped_region in UK_REGIONS.items():
        if keyword in loc_lower:
            region = mapped_region
            break

    # If no keyword match, try postcode matching
    if not region:
        # Extract postcode-like pattern (letters at start of string or after space)
        postcode_match = re.match(r'^([a-zA-Z]{1,2})\d', loc_lower.replace(" ", ""))
        if postcode_match:
            prefix = postcode_match.group(1).lower()
            # Try 2-letter prefix first, then 1-letter
            region = POSTCODE_REGIONS.get(prefix) or POSTCODE_REGIONS.get(prefix[0])
            if region:
                city = location_str.strip()  # Use full string as city for postcode entries

    if not region:
        region = "Other UK"

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


def extract_skills(description_clean: str, title: str = "", search_term: str = "") -> list:
    """Extracts matching skills from a cleaned job description + title + search term.
    Combines all text sources to maximise extraction from short API descriptions.
    """
    # Combine all text sources for matching
    combined_text = f"{title.lower()} {search_term.lower()} {description_clean}"
    
    found = []
    for skill, pattern in _SKILL_PATTERNS.items():
        if pattern.search(combined_text):
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

        skills = extract_skills(description_clean, title_original, job["search_term"] or "")

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
