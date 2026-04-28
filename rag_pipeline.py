"""
JobScope UK — RAG Pipeline

- Load cleaned jobs from SQLite
- Turn each job into an enriched text document
- Embed documents with Gemini
- Store them in ChromaDB
- Retrieve relevant job postings for a user query
- Generate grounded answers using Gemini

Usage examples:
    python rag_pipeline.py --index
    python rag_pipeline.py --ask "What skills are most common for data scientists in London?"
    python rag_pipeline.py --ask "What do junior data analyst roles usually require?" --role "Data Analyst" --seniority junior
"""

import os
import json
import time
import sqlite3
import argparse
import re
from typing import List, Dict, Any, Optional, Tuple

import chromadb
from dotenv import load_dotenv
from google import genai

from skill_taxonomy import ALL_SKILLS

load_dotenv()

DB_NAME = os.getenv("DB_NAME", "jobscope.db")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
CHROMA_PATH = os.getenv("CHROMA_PATH", "./data/chroma_db")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "uk_jobs")
ANALYTICS_MODEL = os.getenv("ANALYTICS_MODEL", "gemini-2.5-flash-lite")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY is not set in the environment.")

client = genai.Client(api_key=GEMINI_API_KEY)
chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = chroma_client.get_or_create_collection(
    name=COLLECTION_NAME,
    metadata={"hnsw:space": "cosine"}
)

ROUTE_SQL = "sql"
ROUTE_RAG = "rag"
ROUTE_HYBRID = "hybrid"

ALLOWED_FILTER_COLUMNS = {
    "role_category": "role_category",
    "region": "location_region",
    "location_region": "location_region",
    "seniority": "seniority",
    "job_source": "job_source",
    "source": "job_source",
}

GROUP_BY_COLUMNS = {
    "role": "role_category",
    "role_category": "role_category",
    "region": "location_region",
    "location_region": "location_region",
    "seniority": "seniority",
    "source": "job_source",
    "job_source": "job_source",
}

KNOWN_ROLE_ALIASES = {
    "data analysis": "Data Analyst",
    "data analytics": "Data Analyst",
    "business intelligence": "BI Analyst",
    "bi": "BI Analyst",
    "machine learning engineer": "ML Engineer",
    "ml engineer": "ML Engineer",
    "nlp": "NLP Engineer",
    "llm": "LLM Engineer",
}

def get_existing_job_ids() -> set:
    """
    Read already indexed job IDs from Chroma so we can resume indexing
    without duplicating work.
    """
    try:
        total = collection.count()
        if total == 0:
            return set()

        results = collection.get(limit=total, include=["metadatas"])
        metadatas = results.get("metadatas", []) or []

        existing_ids = set()
        for meta in metadatas:
            if meta and "job_id" in meta:
                existing_ids.add(meta["job_id"])

        return existing_ids

    except Exception as e:
        print(f"Warning: could not read existing indexed IDs from Chroma: {e}")
        return set()


def show_collection_status() -> None:
    """Quick status check for how many docs are already indexed."""
    try:
        total = collection.count()
        print(f"Current indexed documents in Chroma: {total}")
    except Exception as e:
        print(f"Could not read collection count: {e}")

# ── Database Loading ───────────────────────────────────────────────────

def load_clean_jobs(
    limit: Optional[int] = None,
    source_only: Optional[str] = None,
    exclude_indexed: bool = True
) -> List[Dict[str, Any]]:
    """Load cleaned jobs from SQLite, optionally filtering by source and excluding already indexed jobs."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT
            id,
            job_source,
            title_original,
            title_normalized,
            role_category,
            company,
            description_clean,
            location_raw,
            location_city,
            location_region,
            salary_min,
            salary_max,
            salary_mid,
            has_real_salary,
            extracted_skills,
            date_posted,
            seniority
        FROM clean_jobs
        WHERE description_clean IS NOT NULL
          AND TRIM(description_clean) != ''
    """

    params = []

    if source_only:
        query += " AND job_source = ?"
        params.append(source_only)

    rows = conn.execute(query, params).fetchall()
    conn.close()

    jobs = []
    for row in rows:
        job = dict(row)

        try:
            job["extracted_skills"] = json.loads(job["extracted_skills"]) if job["extracted_skills"] else []
        except json.JSONDecodeError:
            job["extracted_skills"] = []

        jobs.append(job)

    if exclude_indexed:
        existing_ids = get_existing_job_ids()
        jobs = [job for job in jobs if job["id"] not in existing_ids]

    if limit:
        jobs = jobs[:limit]

    return jobs

# ── Document Construction ──────────────────────────────────────────────

def format_salary(job: Dict[str, Any]) -> str:
    """Return readable salary text."""
    if job["salary_min"] and job["salary_max"]:
        return f"£{job['salary_min']:,.0f} - £{job['salary_max']:,.0f}"
    if job["salary_mid"]:
        return f"Approx. £{job['salary_mid']:,.0f}"
    return "Not specified"


def create_rag_document(job: Dict[str, Any], max_description_chars: int = 4500) -> str:
    """
    Turn a clean_jobs row into an enriched text document for retrieval.
    Combines structured fields and the original cleaned description.
    """
    skills = ", ".join(job["extracted_skills"]) if job["extracted_skills"] else "None extracted"
    description = (job["description_clean"] or "")[:max_description_chars]

    doc = f"""
        Job ID: {job['id']}
        Source: {job.get('job_source', 'Unknown')}
        Original Title: {job.get('title_original', 'Unknown')}
        Normalised Title: {job.get('title_normalized', 'Unknown')}
        Role Category: {job.get('role_category', 'Unknown')}
        Company: {job.get('company', 'Unknown')}
        Location: {job.get('location_city', 'Unknown')}, {job.get('location_region', 'Unknown')}
        Seniority: {job.get('seniority', 'Unknown')}
        Salary: {format_salary(job)}
        Date Posted: {job.get('date_posted', 'Unknown')}
        Extracted Skills: {skills}

        Job Description:
        {description}
        """.strip()

    return doc


# ── Embeddings ─────────────────────────────────────────────────────────

def get_embedding(text: str) -> Tuple[Optional[List[float]], Optional[str]]:
    """Generate a Gemini embedding for a single text."""
    try:
        response = client.models.embed_content(
            model="gemini-embedding-001",
            contents=text
        )
        return response.embeddings[0].values, None
    except Exception as e:
        error_text = str(e)
        print(f"Embedding error: {error_text}")
        return None, error_text

# ── Indexing ───────────────────────────────────────────────────────────

def reset_collection() -> None:
    """Delete and recreate the Chroma collection."""
    global collection
    try:
        chroma_client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass

    collection = chroma_client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"}
    )

def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split a list into smaller chunks."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

def index_jobs(
    limit: Optional[int] = None,
    reset: bool = False,
    batch_size: int = 100,
    source_only: Optional[str] = None
) -> None:
    """
    Index jobs into ChromaDB.

    Features:
    - optional source filter (e.g. Reed only)
    - skips already indexed jobs
    - processes in small batches
    - resumable if quota runs out
    """
    if reset:
        print("Resetting Chroma collection...")
        reset_collection()

    show_collection_status()

    jobs = load_clean_jobs(
        limit=limit,
        source_only=source_only,
        exclude_indexed=True
    )

    print(f"Jobs selected for indexing: {len(jobs)}")
    if source_only:
        print(f"Source filter applied: {source_only}")

    if not jobs:
        print("No new jobs found to index.")
        return

    job_batches = chunk_list(jobs, batch_size)

    indexed_this_run = 0
    failed_this_run = 0

    for batch_num, batch in enumerate(job_batches, start=1):
        print(f"\nProcessing batch {batch_num}/{len(job_batches)} ({len(batch)} jobs)...")

        documents = []
        embeddings = []
        ids = []
        metadatas = []

        for job in batch:
            doc_text = create_rag_document(job)

            embedding, error_text = get_embedding(doc_text)

            if embedding is None:
                failed_this_run += 1

                # Stop the run early if quota is exhausted repeatedly
                if error_text and "429" in error_text and "RESOURCE_EXHAUSTED" in error_text:
                    print("\nQuota exhausted. Stopping this indexing run early to preserve time.")
                    break

                continue

            documents.append(doc_text)
            embeddings.append(embedding)
            ids.append(f"job_{job['id']}")
            metadatas.append({
                "job_id": job["id"],
                "job_source": job.get("job_source", "Unknown"),
                "role_category": job.get("role_category", "Unknown"),
                "location_region": job.get("location_region", "Unknown"),
                "seniority": job.get("seniority", "Unknown"),
                "company": job.get("company", "Unknown"),
                "has_real_salary": int(job.get("has_real_salary", 0) or 0),
                "salary_mid": float(job["salary_mid"]) if job.get("salary_mid") is not None else 0.0,
            })

            # Be gentle with quotas
            time.sleep(5.0)

        if failed_this_run > 0 and len(documents) < len(batch):
            # If we broke out early because of quota exhaustion, still add successful docs
            pass

        if documents:
            try:
                collection.add(
                    documents=documents,
                    embeddings=embeddings,
                    ids=ids,
                    metadatas=metadatas
                )
                indexed_this_run += len(documents)
                print(f"Added {len(documents)} documents from batch {batch_num}.")
            except Exception as e:
                print(f"Failed to add batch {batch_num} to Chroma: {e}")

        print(f"Running totals -> indexed: {indexed_this_run}, failed embeddings: {failed_this_run}")

    print("\nIndexing run complete.")
    show_collection_status()

# ── Retrieval ──────────────────────────────────────────────────────────

def build_where_filter(
    role: Optional[str] = None,
    region: Optional[str] = None,
    seniority: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Build an optional Chroma metadata filter."""
    clauses = []

    if role:
        clauses.append({"role_category": {"$eq": role}})
    if region:
        clauses.append({"location_region": {"$eq": region}})
    if seniority:
        clauses.append({"seniority": {"$eq": seniority}})

    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]

    return {"$and": clauses}


def retrieve(
    query: str,
    n_results: int = 5,
    role: Optional[str] = None,
    region: Optional[str] = None,
    seniority: Optional[str] = None
) -> Dict[str, Any]:
    """Retrieve most relevant documents for a query."""
    query_embedding, error_text = get_embedding(query)

    if query_embedding is None:
        raise ValueError(f"Failed to embed query: {error_text}")

    where_filter = build_where_filter(role=role, region=region, seniority=seniority)

    kwargs = {
        "query_embeddings": [query_embedding],
        "n_results": n_results
    }

    if where_filter:
        kwargs["where"] = where_filter

    results = collection.query(**kwargs)
    return results

def inspect_collection_sample(limit: int = 5) -> None:
    """Print a small sample of indexed documents and metadata."""
    try:
        results = collection.get(limit=limit, include=["documents", "metadatas"])
        docs = results.get("documents", [])
        metas = results.get("metadatas", [])

        print(f"\nShowing {len(docs)} sample indexed documents:\n")
        for i, (doc, meta) in enumerate(zip(docs, metas), start=1):
            print("=" * 80)
            print(f"SAMPLE {i}")
            print("=" * 80)
            print("Metadata:", meta)
            print("\nDocument preview:")
            print(doc[:700])
            print("\n")
    except Exception as e:
        print(f"Could not inspect collection sample: {e}")

def preview_retrieval(
    question: str,
    n_results: int = 3,
    role: Optional[str] = None,
    region: Optional[str] = None,
    seniority: Optional[str] = None
) -> None:
    """Show retrieved documents before generation."""
    results = retrieve(
        query=question,
        n_results=n_results,
        role=role,
        region=region,
        seniority=seniority
    )

    docs = results.get("documents", [[]])[0]
    metas = results.get("metadatas", [[]])[0]
    ids = results.get("ids", [[]])[0]

    if not docs:
        print("No documents retrieved.")
        return

    print(f"\nRetrieved {len(docs)} documents for question: {question}\n")

    for i, (doc_id, meta, doc) in enumerate(zip(ids, metas, docs), start=1):
        print("=" * 100)
        print(f"RESULT {i}: {doc_id}")
        print("=" * 100)
        print("Metadata:", meta)
        print("\nPreview:")
        print(doc[:1000])
        print("\n")

# ── SQL Analytics ─────────────────────────────────────────────────────

def get_db_connection() -> sqlite3.Connection:
    """Open SQLite with dict-like rows."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def get_distinct_values(column: str) -> List[str]:
    """Read known values for a controlled clean_jobs column."""
    if column not in set(ALLOWED_FILTER_COLUMNS.values()) | set(GROUP_BY_COLUMNS.values()):
        return []

    conn = get_db_connection()
    rows = conn.execute(
        f"""
        SELECT DISTINCT {column}
        FROM clean_jobs
        WHERE {column} IS NOT NULL
          AND TRIM({column}) != ''
        ORDER BY {column}
        """
    ).fetchall()
    conn.close()
    return [row[0] for row in rows]


def canonicalize_known_value(value: str, known_values: List[str]) -> Optional[str]:
    """Match user/model text to one of the values present in the DB."""
    if not value:
        return None

    value_clean = str(value).strip()
    value_lower = value_clean.lower()

    if value_lower in KNOWN_ROLE_ALIASES:
        value_clean = KNOWN_ROLE_ALIASES[value_lower]
        value_lower = value_clean.lower()

    for known in known_values:
        if value_lower == known.lower():
            return known

    for known in known_values:
        known_lower = known.lower()
        if re.search(rf"\b{re.escape(known_lower)}s?\b", value_lower):
            return known
        if re.search(rf"\b{re.escape(known_lower)}s?\b", value_lower.replace("-", " ")):
            return known

    return None


def extract_known_values_from_question(question: str, column: str) -> List[str]:
    """Find DB-backed values, such as role categories or regions, in a question."""
    question_lower = question.lower()
    matches = []

    for alias, canonical in KNOWN_ROLE_ALIASES.items():
        if column == "role_category" and re.search(rf"\b{re.escape(alias)}s?\b", question_lower):
            matches.append(canonical)

    for value in get_distinct_values(column):
        value_lower = value.lower()
        if re.search(rf"\b{re.escape(value_lower)}s?\b", question_lower):
            matches.append(value)

    seen = set()
    unique_matches = []
    for match in matches:
        if match not in seen:
            seen.add(match)
            unique_matches.append(match)

    return unique_matches


def extract_skill_from_question(question: str) -> Optional[str]:
    """Find the most specific taxonomy skill mentioned in a question."""
    question_lower = question.lower()
    for skill in sorted(ALL_SKILLS, key=len, reverse=True):
        skill_lower = skill.lower()
        if re.search(rf"(?<![a-z0-9]){re.escape(skill_lower)}(?![a-z0-9])", question_lower):
            return skill
    return None


def merge_explicit_filters(
    planned_filters: Dict[str, Any],
    role: Optional[str] = None,
    region: Optional[str] = None,
    seniority: Optional[str] = None,
) -> Dict[str, Any]:
    """Apply CLI filters as authoritative constraints over routed filters."""
    filters = dict(planned_filters or {})
    if role:
        filters["role_category"] = role
    if region:
        filters["location_region"] = region
    if seniority:
        filters["seniority"] = seniority
    return filters


def normalize_filters(filters: Dict[str, Any]) -> Tuple[Dict[str, str], List[str]]:
    """Keep only safe filter columns and known DB values."""
    normalized = {}
    notes = []

    for raw_key, raw_value in (filters or {}).items():
        column = ALLOWED_FILTER_COLUMNS.get(raw_key)
        if not column or raw_value in (None, "", []):
            continue

        value = raw_value[0] if isinstance(raw_value, list) and raw_value else raw_value
        known_value = canonicalize_known_value(str(value), get_distinct_values(column))
        if known_value:
            normalized[column] = known_value
        else:
            notes.append(f"Ignored unknown {column} filter: {value}")

    return normalized, notes


def build_sql_filter_clause(filters: Dict[str, str], alias: str = "j") -> Tuple[str, List[Any]]:
    """Build a parameterised WHERE suffix for allowed clean_jobs filters."""
    clauses = []
    params = []

    for column, value in filters.items():
        clauses.append(f"{alias}.{column} = ?")
        params.append(value)

    if not clauses:
        return "", params

    return " AND " + " AND ".join(clauses), params


def parse_limit(question: str, default: int = 10) -> int:
    """Extract a sensible LIMIT from user wording."""
    match = re.search(r"\btop\s+(\d{1,2})\b", question.lower())
    if match:
        return max(1, min(int(match.group(1)), 50))
    return default


def classify_query_heuristic(question: str) -> Dict[str, Any]:
    """Deterministic classifier for common analytics and hybrid questions."""
    q = question.lower()
    filters: Dict[str, Any] = {}

    roles = extract_known_values_from_question(question, "role_category")
    regions = extract_known_values_from_question(question, "location_region")
    seniorities = extract_known_values_from_question(question, "seniority")
    sources = extract_known_values_from_question(question, "job_source")

    if len(roles) == 1:
        filters["role_category"] = roles[0]
    if len(regions) == 1:
        filters["location_region"] = regions[0]
    if len(seniorities) == 1:
        filters["seniority"] = seniorities[0]
    if len(sources) == 1:
        filters["job_source"] = sources[0]

    skill = extract_skill_from_question(question)
    limit = parse_limit(question)
    asks_for_examples = any(term in q for term in ["example", "posting", "job description", "requirements", "require"])
    analytical_terms = [
        "top", "most common", "common", "frequent", "popular", "highest", "lowest",
        "average", "median", "salary", "pay", "count", "how many", "number of",
        "compare", " vs ", "versus", "breakdown", "distribution", "by region",
        "by role", "by seniority", "by source",
    ]

    if any(term in q for term in ["compare", " vs ", "versus", "difference between"]) and skill:
        return {
            "route": ROUTE_SQL,
            "intent": "skill_comparison",
            "filters": filters,
            "metric": "skill_demand",
            "group_by": "role_category" if len(roles) >= 2 or "role" in q else "location_region",
            "compare_values": roles if len(roles) >= 2 else [],
            "skill": skill,
            "limit": limit,
        }

    if skill and any(term in q for term in ["how many", "count", "number of"]) and any(term in q for term in ["mention", "with", "use", "require", "need"]):
        return {
            "route": ROUTE_SQL,
            "intent": "skill_count",
            "filters": filters,
            "metric": "skill_demand",
            "group_by": None,
            "skill": skill,
            "limit": limit,
        }

    if "skill" in q and any(term in q for term in ["top", "most common", "common", "frequent", "popular", "mention"]):
        return {
            "route": ROUTE_HYBRID if asks_for_examples or "usually" in q else ROUTE_SQL,
            "intent": "top_skills",
            "filters": filters,
            "metric": "job_count",
            "group_by": "skill",
            "limit": limit,
        }

    if any(term in q for term in ["salary", "pay", "compensation", "wage"]):
        group_by = None
        for phrase, column in [
            ("by role", "role_category"),
            ("which role", "role_category"),
            ("by region", "location_region"),
            ("which region", "location_region"),
            ("by seniority", "seniority"),
            ("by source", "job_source"),
        ]:
            if phrase in q:
                group_by = column
                break

        return {
            "route": ROUTE_SQL,
            "intent": "salary_by_group" if group_by else "salary_summary",
            "filters": filters,
            "metric": "median_salary" if "median" in q or "highest" in q else "salary",
            "group_by": group_by,
            "limit": limit,
        }

    if any(term in q for term in ["how many", "count", "number of", "breakdown", "distribution", "most common role"]):
        group_by = None
        for phrase, column in [
            ("role", "role_category"),
            ("region", "location_region"),
            ("location", "location_region"),
            ("seniority", "seniority"),
            ("source", "job_source"),
        ]:
            if phrase in q:
                group_by = column
                break

        return {
            "route": ROUTE_SQL,
            "intent": "counts_by_group" if group_by else "count_jobs",
            "filters": filters,
            "metric": "job_count",
            "group_by": group_by,
            "limit": limit,
        }

    if asks_for_examples and any(term in q for term in analytical_terms):
        return {
            "route": ROUTE_HYBRID,
            "intent": "top_skills" if "skill" in q else "count_jobs",
            "filters": filters,
            "metric": "job_count",
            "group_by": "skill" if "skill" in q else None,
            "limit": limit,
        }

    if any(term in q for term in analytical_terms):
        return {
            "route": ROUTE_SQL,
            "intent": "top_skills" if "skill" in q else "count_jobs",
            "filters": filters,
            "metric": "job_count",
            "group_by": "skill" if "skill" in q else None,
            "limit": limit,
        }

    return {
        "route": ROUTE_RAG,
        "intent": "qualitative_context",
        "filters": filters,
        "metric": None,
        "group_by": None,
        "limit": limit,
    }


def extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    """Best-effort extraction of a JSON object from model text."""
    if not text:
        return None

    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?", "", cleaned).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()

    try:
        parsed = json.loads(cleaned)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if not match:
        return None

    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None


def classify_query_with_gemini(question: str) -> Optional[Dict[str, Any]]:
    """Ask Gemini for a constrained routing decision."""
    role_values = ", ".join(get_distinct_values("role_category"))
    region_values = ", ".join(get_distinct_values("location_region"))
    seniority_values = ", ".join(get_distinct_values("seniority"))
    source_values = ", ".join(get_distinct_values("job_source"))

    prompt = f"""
Classify this JobScope question into a constrained analytics/RAG plan.

Return ONLY valid JSON with these keys:
route: one of ["sql", "rag", "hybrid"]
intent: one of ["top_skills", "skill_comparison", "skill_count", "salary_summary", "salary_by_group", "counts_by_group", "count_jobs", "qualitative_context"]
filters: object using only role_category, location_region, seniority, job_source
metric: "job_count", "skill_demand", "salary", "median_salary", or null
group_by: one of "skill", "role_category", "location_region", "seniority", "job_source", or null
limit: integer from 1 to 50
skill: a specific skill if the question compares or filters one skill, otherwise null
compare_values: list of role/region/seniority/source values to compare, otherwise []

Use sql for representative aggregate questions.
Use rag for qualitative questions about role requirements or examples.
Use hybrid when the question needs an aggregate answer plus example postings.

Known role_category values: {role_values}
Known location_region values: {region_values}
Known seniority values: {seniority_values}
Known job_source values: {source_values}

Question: {question}
""".strip()

    try:
        response = client.models.generate_content(
            model=ANALYTICS_MODEL,
            contents=prompt
        )
        return extract_json_object(response.text)
    except Exception as e:
        print(f"Warning: query router model failed, using heuristic router: {e}")
        return None


def sanitize_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Clamp model/heuristic output to supported routes, intents, columns, and limits."""
    safe = dict(plan or {})
    safe["route"] = safe.get("route") if safe.get("route") in {ROUTE_SQL, ROUTE_RAG, ROUTE_HYBRID} else ROUTE_RAG

    supported_intents = {
        "top_skills", "skill_comparison", "salary_summary", "salary_by_group",
        "counts_by_group", "count_jobs", "skill_count", "qualitative_context",
    }
    safe["intent"] = safe.get("intent") if safe.get("intent") in supported_intents else "qualitative_context"

    group_by = safe.get("group_by")
    if group_by == "skill":
        safe["group_by"] = "skill"
    else:
        safe["group_by"] = GROUP_BY_COLUMNS.get(group_by)

    try:
        safe["limit"] = max(1, min(int(safe.get("limit", 10)), 50))
    except (TypeError, ValueError):
        safe["limit"] = 10

    filters, notes = normalize_filters(safe.get("filters", {}))
    safe["filters"] = filters
    safe["notes"] = notes

    skill = safe.get("skill")
    if skill:
        safe["skill"] = extract_skill_from_question(str(skill)) or canonicalize_known_value(str(skill), ALL_SKILLS)
    else:
        safe["skill"] = None

    compare_values = safe.get("compare_values") or []
    safe["compare_values"] = [str(value) for value in compare_values if value]

    return safe


def classify_query(
    question: str,
    role: Optional[str] = None,
    region: Optional[str] = None,
    seniority: Optional[str] = None,
) -> Dict[str, Any]:
    """Classify a question, using Gemini when useful and a deterministic fallback."""
    heuristic_plan = classify_query_heuristic(question)
    q = question.lower()
    analytical_hint = any(term in q for term in [
        "top", "count", "how many", "salary", "pay", "average", "median",
        "compare", "versus", " vs ", "most common", "breakdown", "distribution",
        "highest", "lowest", "frequent", "popular",
    ])

    # Obvious SQL/hybrid intents are safer to keep deterministic; vague questions can use Gemini.
    model_plan = None
    if heuristic_plan["route"] == ROUTE_RAG and analytical_hint:
        model_plan = classify_query_with_gemini(question)

    plan = sanitize_plan(model_plan or heuristic_plan)
    plan["filters"] = merge_explicit_filters(plan.get("filters", {}), role=role, region=region, seniority=seniority)
    plan["filters"], extra_notes = normalize_filters(plan["filters"])
    plan["notes"].extend(extra_notes)

    if plan["route"] in {ROUTE_SQL, ROUTE_HYBRID} and plan["intent"] == "qualitative_context":
        plan["route"] = ROUTE_RAG

    return plan


def count_filtered_jobs(filters: Dict[str, str]) -> int:
    """Count jobs under a set of clean_jobs filters."""
    where_sql, params = build_sql_filter_clause(filters, alias="j")
    conn = get_db_connection()
    count = conn.execute(
        f"SELECT COUNT(*) FROM clean_jobs j WHERE 1=1{where_sql}",
        params
    ).fetchone()[0]
    conn.close()
    return int(count)


def execute_top_skills(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Run an approved top-skills aggregate using SQLite JSON expansion."""
    filters = plan["filters"]
    limit = plan["limit"]
    sample_size = count_filtered_jobs(filters)
    where_sql, params = build_sql_filter_clause(filters, alias="j")
    sql = f"""
        SELECT
            skill.value AS skill,
            COUNT(*) AS job_count,
            ROUND(100.0 * COUNT(*) / NULLIF(?, 0), 1) AS pct_jobs
        FROM clean_jobs j, json_each(j.extracted_skills) AS skill
        WHERE 1=1{where_sql}
        GROUP BY skill.value
        ORDER BY job_count DESC, skill.value ASC
        LIMIT ?
    """.strip()
    query_params = [sample_size] + params + [limit]

    conn = get_db_connection()
    rows = [dict(row) for row in conn.execute(sql, query_params).fetchall()]
    conn.close()

    return {
        "kind": "sql",
        "intent": "top_skills",
        "rows": rows,
        "sample_size": sample_size,
        "sql": sql,
        "params": query_params,
        "filters": filters,
        "notes": list(plan.get("notes", [])),
    }


def execute_counts(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Run approved job count and grouped-count queries."""
    filters = plan["filters"]
    group_by = plan.get("group_by")
    limit = plan["limit"]
    sample_size = count_filtered_jobs(filters)
    where_sql, params = build_sql_filter_clause(filters, alias="j")

    if group_by:
        sql = f"""
            SELECT
                j.{group_by} AS {group_by},
                COUNT(*) AS job_count,
                ROUND(100.0 * COUNT(*) / NULLIF(?, 0), 1) AS pct_jobs
            FROM clean_jobs j
            WHERE 1=1{where_sql}
            GROUP BY j.{group_by}
            ORDER BY job_count DESC, j.{group_by} ASC
            LIMIT ?
        """.strip()
        query_params = [sample_size] + params + [limit]
    else:
        sql = f"SELECT COUNT(*) AS job_count FROM clean_jobs j WHERE 1=1{where_sql}"
        query_params = params

    conn = get_db_connection()
    rows = [dict(row) for row in conn.execute(sql, query_params).fetchall()]
    conn.close()

    return {
        "kind": "sql",
        "intent": "counts_by_group" if group_by else "count_jobs",
        "rows": rows,
        "sample_size": sample_size,
        "sql": sql,
        "params": query_params,
        "filters": filters,
        "notes": list(plan.get("notes", [])),
    }


def median(values: List[float]) -> Optional[float]:
    """Calculate median without requiring SQLite extension functions."""
    if not values:
        return None
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2


def execute_salary(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Run salary summary/grouping with Python median calculation."""
    filters = dict(plan["filters"])
    group_by = plan.get("group_by")
    limit = plan["limit"]
    where_sql, params = build_sql_filter_clause(filters, alias="j")
    base_where = f"j.salary_mid IS NOT NULL AND j.has_real_salary = 1{where_sql}"

    select_group = f"j.{group_by} AS group_value, " if group_by else ""
    order_group = f", j.{group_by}" if group_by else ""
    sql = f"""
        SELECT {select_group}j.salary_mid
        FROM clean_jobs j
        WHERE {base_where}
        ORDER BY j.salary_mid{order_group}
    """.strip()

    conn = get_db_connection()
    raw_rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    conn.close()

    grouped: Dict[str, List[float]] = {}
    if group_by:
        for row in raw_rows:
            grouped.setdefault(row["group_value"], []).append(float(row["salary_mid"]))
    else:
        grouped["All matching jobs"] = [float(row["salary_mid"]) for row in raw_rows]

    rows = []
    for label, salaries in grouped.items():
        rows.append({
            group_by or "scope": label,
            "salary_count": len(salaries),
            "median_salary": round(median(salaries) or 0),
            "avg_salary": round(sum(salaries) / len(salaries)) if salaries else 0,
            "min_salary": round(min(salaries)) if salaries else 0,
            "max_salary": round(max(salaries)) if salaries else 0,
        })

    rows.sort(key=lambda row: row["median_salary"], reverse=True)
    rows = rows[:limit]

    sample_size = len(raw_rows)
    notes = list(plan.get("notes", []))
    notes.append("Salary analysis uses only rows with salary_mid and has_real_salary = 1.")

    return {
        "kind": "sql",
        "intent": "salary_by_group" if group_by else "salary_summary",
        "rows": rows,
        "sample_size": sample_size,
        "sql": sql,
        "params": params,
        "filters": filters,
        "notes": notes,
    }


def execute_skill_comparison(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Compare one skill's demand across roles/regions/seniority/source."""
    skill = plan.get("skill")
    if not skill:
        return execute_top_skills({**plan, "intent": "top_skills"})

    filters = dict(plan["filters"])
    group_by = plan.get("group_by") or "role_category"
    limit = plan["limit"]
    compare_values = plan.get("compare_values", [])

    if compare_values:
        known_values = get_distinct_values(group_by)
        canonical_values = [
            canonicalize_known_value(value, known_values)
            for value in compare_values
        ]
        canonical_values = [value for value in canonical_values if value]
    else:
        canonical_values = []

    where_sql, params = build_sql_filter_clause(filters, alias="j")
    value_sql = ""
    value_params: List[Any] = []
    if canonical_values:
        placeholders = ", ".join("?" for _ in canonical_values)
        value_sql = f" AND j.{group_by} IN ({placeholders})"
        value_params = canonical_values

    sql = f"""
        SELECT
            j.{group_by} AS {group_by},
            COUNT(*) AS total_jobs,
            SUM(
                CASE WHEN EXISTS (
                    SELECT 1
                    FROM json_each(j.extracted_skills) AS skill
                    WHERE LOWER(skill.value) = LOWER(?)
                )
                THEN 1 ELSE 0 END
            ) AS jobs_with_skill,
            ROUND(
                100.0 * SUM(
                    CASE WHEN EXISTS (
                        SELECT 1
                        FROM json_each(j.extracted_skills) AS skill
                        WHERE LOWER(skill.value) = LOWER(?)
                    )
                    THEN 1 ELSE 0 END
                ) / NULLIF(COUNT(*), 0),
                1
            ) AS pct_jobs
        FROM clean_jobs j
        WHERE 1=1{where_sql}{value_sql}
        GROUP BY j.{group_by}
        ORDER BY pct_jobs DESC, jobs_with_skill DESC, j.{group_by} ASC
        LIMIT ?
    """.strip()
    query_params = [skill, skill] + params + value_params + [limit]

    conn = get_db_connection()
    rows = [dict(row) for row in conn.execute(sql, query_params).fetchall()]
    conn.close()

    return {
        "kind": "sql",
        "intent": "skill_comparison",
        "rows": rows,
        "sample_size": sum(row["total_jobs"] for row in rows),
        "sql": sql,
        "params": query_params,
        "filters": filters,
        "notes": list(plan.get("notes", [])),
        "skill": skill,
    }


def execute_skill_count(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Count matching jobs that mention one specific skill."""
    skill = plan.get("skill")
    filters = dict(plan["filters"])
    where_sql, params = build_sql_filter_clause(filters, alias="j")
    sample_size = count_filtered_jobs(filters)

    sql = f"""
        SELECT
            ? AS skill,
            COUNT(*) AS total_jobs,
            SUM(
                CASE WHEN EXISTS (
                    SELECT 1
                    FROM json_each(j.extracted_skills) AS skill_item
                    WHERE LOWER(skill_item.value) = LOWER(?)
                )
                THEN 1 ELSE 0 END
            ) AS jobs_with_skill,
            ROUND(
                100.0 * SUM(
                    CASE WHEN EXISTS (
                        SELECT 1
                        FROM json_each(j.extracted_skills) AS skill_item
                        WHERE LOWER(skill_item.value) = LOWER(?)
                    )
                    THEN 1 ELSE 0 END
                ) / NULLIF(COUNT(*), 0),
                1
            ) AS pct_jobs
        FROM clean_jobs j
        WHERE 1=1{where_sql}
    """.strip()
    query_params = [skill, skill, skill] + params

    conn = get_db_connection()
    rows = [dict(row) for row in conn.execute(sql, query_params).fetchall()]
    conn.close()

    return {
        "kind": "sql",
        "intent": "skill_count",
        "rows": rows,
        "sample_size": sample_size,
        "sql": sql,
        "params": query_params,
        "filters": filters,
        "notes": list(plan.get("notes", [])),
        "skill": skill,
    }


def execute_sql_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatch only to approved SQL query builders."""
    intent = plan["intent"]
    if intent == "top_skills":
        return execute_top_skills(plan)
    if intent in {"count_jobs", "counts_by_group"}:
        return execute_counts(plan)
    if intent in {"salary_summary", "salary_by_group"}:
        return execute_salary(plan)
    if intent == "skill_comparison":
        return execute_skill_comparison(plan)
    if intent == "skill_count":
        return execute_skill_count(plan)

    return {
        "kind": "sql",
        "intent": intent,
        "rows": [],
        "sample_size": 0,
        "sql": "",
        "params": [],
        "filters": plan.get("filters", {}),
        "notes": ["No approved SQL template matched this question."],
    }


def format_value(value: Any, column: Optional[str] = None) -> str:
    """Readable value formatting for CLI output."""
    if value is None:
        return ""
    money_columns = {"median_salary", "avg_salary", "min_salary", "max_salary", "salary_mid"}
    if column in money_columns and isinstance(value, (int, float)):
        return f"£{value:,.0f}"
    return str(value)


def format_result_table(rows: List[Dict[str, Any]]) -> str:
    """Render a compact markdown-style table."""
    if not rows:
        return "No rows returned."

    columns = list(rows[0].keys())
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = []
    for row in rows:
        body.append("| " + " | ".join(format_value(row.get(col), col) for col in columns) + " |")

    return "\n".join([header, separator] + body)


def describe_filters(filters: Dict[str, str]) -> str:
    """Human-readable filter summary."""
    if not filters:
        return "all clean jobs"
    labels = {
        "role_category": "role",
        "location_region": "region",
        "seniority": "seniority",
        "job_source": "source",
    }
    return ", ".join(f"{labels.get(key, key)}={value}" for key, value in filters.items())


def build_sql_answer(question: str, sql_result: Dict[str, Any], plan: Dict[str, Any]) -> str:
    """Create a transparent, non-LLM answer from SQL rows."""
    rows = sql_result.get("rows", [])
    intent = sql_result.get("intent")
    filters_text = describe_filters(sql_result.get("filters", {}))
    sample_size = sql_result.get("sample_size", 0)

    if not rows:
        direct = "I could not find matching rows for that analytical query."
    elif intent == "top_skills":
        top = rows[0]
        direct = (
            f"The top skill for {filters_text} is {top['skill']} "
            f"({top['job_count']} jobs, {top['pct_jobs']}% of matching postings)."
        )
    elif intent in {"skill_comparison", "skill_count"}:
        skill = sql_result.get("skill", plan.get("skill", "the requested skill"))
        top = rows[0]
        if intent == "skill_count":
            direct = (
                f"{top['jobs_with_skill']} of {top['total_jobs']} matching jobs mention {skill} "
                f"({top['pct_jobs']}%)."
            )
        else:
            group_column = next((key for key in top.keys() if key not in {"total_jobs", "jobs_with_skill", "pct_jobs"}), "group")
            direct = (
                f"{skill} demand is highest for {top[group_column]} in this comparison "
                f"({top['jobs_with_skill']} of {top['total_jobs']} jobs, {top['pct_jobs']}%)."
            )
    elif intent in {"salary_summary", "salary_by_group"}:
        top = rows[0]
        label_column = next((key for key in top.keys() if key not in {"salary_count", "median_salary", "avg_salary", "min_salary", "max_salary"}), "scope")
        direct = (
            f"The highest median salary result is {top[label_column]} at "
            f"£{top['median_salary']:,.0f}, based on {top['salary_count']} salary-bearing postings."
        )
    elif intent == "count_jobs":
        direct = f"There are {rows[0]['job_count']} matching jobs for {filters_text}."
    else:
        top = rows[0]
        label_column = next((key for key in top.keys() if key not in {"job_count", "pct_jobs"}), "group")
        direct = (
            f"The largest group is {top[label_column]} with {top['job_count']} jobs "
            f"({top.get('pct_jobs', 100)}% of matching postings)."
        )

    notes = sql_result.get("notes", [])
    notes_text = "\n".join(f"- {note}" for note in notes) if notes else "- None"

    return f"""
Answer:
- {direct}

Result table:
{format_result_table(rows)}

Dataset basis:
- Filters used: {filters_text}
- Matching sample size: {sample_size}

Notes:
{notes_text}
""".strip()


# ── Generation ────────────────────────────────────────────────────────

def build_prompt(question: str, retrieved_docs: List[str], retrieved_meta: List[Dict[str, Any]]) -> str:
    """Build a grounded prompt for Gemini."""
    context_blocks = []

    for i, (doc, meta) in enumerate(zip(retrieved_docs, retrieved_meta), start=1):
        source_label = (
            f"Source {i} | role={meta.get('role_category')} | "
            f"region={meta.get('location_region')} | "
            f"seniority={meta.get('seniority')} | "
            f"company={meta.get('company')}"
        )
        context_blocks.append(f"{source_label}\n{doc}")

    context = "\n\n" + ("\n\n---\n\n".join(context_blocks))

    prompt = f"""
You are a UK job market analyst helping a user understand real job postings.

Answer the question using ONLY the retrieved job-posting context below.
Do not invent facts not supported by the context.
Be specific and practical.
Where possible, summarise patterns across the retrieved postings rather than just repeating one posting.
If the evidence is limited or mixed, say so clearly.

Retrieved Context:
{context}

User Question:
{question}

Return your answer in this format:

Answer:
- A clear, concise answer in paragraph form.

Evidence from postings:
- 3 to 5 bullet points summarising the strongest grounded evidence.

Source summary:
- Briefly mention the kinds of roles / regions / companies the answer was based on.
""".strip()

    return prompt


def build_hybrid_prompt(
    question: str,
    sql_answer: str,
    retrieved_docs: List[str],
    retrieved_meta: List[Dict[str, Any]],
) -> str:
    """Build a prompt that combines representative SQL results with retrieved examples."""
    context_blocks = []

    for i, (doc, meta) in enumerate(zip(retrieved_docs, retrieved_meta), start=1):
        source_label = (
            f"Example posting {i} | role={meta.get('role_category')} | "
            f"region={meta.get('location_region')} | "
            f"seniority={meta.get('seniority')} | "
            f"company={meta.get('company')}"
        )
        context_blocks.append(f"{source_label}\n{doc}")

    examples = "\n\n" + ("\n\n---\n\n".join(context_blocks)) if context_blocks else "No example postings retrieved."

    prompt = f"""
You are a UK job market analyst helping a user understand real job postings.

Use the SQL analytics result as the representative dataset-wide evidence.
Use the retrieved postings only as qualitative examples.
Do not imply that the retrieved postings alone represent the whole dataset.

SQL Analytics Result:
{sql_answer}

Retrieved Example Postings:
{examples}

User Question:
{question}

Return your answer in this format:

Answer:
- A clear, concise answer that leads with the SQL result.

Dataset-wide evidence:
- 2 to 4 bullets from the SQL result, including sample size or filters where useful.

Examples from postings:
- 2 to 4 bullets from the retrieved postings, clearly framed as examples.
""".strip()

    return prompt


def ask(
    question: str,
    n_results: int = 5,
    role: Optional[str] = None,
    region: Optional[str] = None,
    seniority: Optional[str] = None,
    show_sql: bool = False
) -> Dict[str, Any]:
    """Route a question to SQL analytics, RAG retrieval, or both."""
    plan = classify_query(question, role=role, region=region, seniority=seniority)

    if plan["route"] == ROUTE_SQL:
        sql_result = execute_sql_plan(plan)
        answer = build_sql_answer(question, sql_result, plan)
        return {
            "answer": answer,
            "route": ROUTE_SQL,
            "plan": plan,
            "sql_result": sql_result,
            "docs": [],
            "metas": [],
            "ids": [],
            "show_sql": show_sql,
        }

    sql_result = None
    sql_answer = None
    if plan["route"] == ROUTE_HYBRID:
        sql_result = execute_sql_plan(plan)
        sql_answer = build_sql_answer(question, sql_result, plan)

    try:
        results = retrieve(
            query=question,
            n_results=n_results,
            role=plan["filters"].get("role_category"),
            region=plan["filters"].get("location_region"),
            seniority=plan["filters"].get("seniority")
        )
    except Exception as e:
        if plan["route"] == ROUTE_HYBRID and sql_answer:
            return {
                "answer": sql_answer + f"\n\nRetrieval note:\n- Retrieved posting examples were unavailable: {e}",
                "route": ROUTE_HYBRID,
                "plan": plan,
                "sql_result": sql_result,
                "docs": [],
                "metas": [],
                "ids": [],
                "show_sql": show_sql,
            }
        raise

    docs = results.get("documents", [[]])[0]
    metas = results.get("metadatas", [[]])[0]
    ids = results.get("ids", [[]])[0]

    if not docs:
        if plan["route"] == ROUTE_HYBRID and sql_answer:
            return {
                "answer": sql_answer + "\n\nRetrieval note:\n- No posting examples were retrieved for the hybrid part of this answer.",
                "route": ROUTE_HYBRID,
                "plan": plan,
                "sql_result": sql_result,
                "docs": [],
                "metas": [],
                "ids": [],
                "show_sql": show_sql,
            }
        return {
            "answer": "No relevant job postings were retrieved for this query.",
            "route": plan["route"],
            "plan": plan,
            "docs": [],
            "metas": [],
            "ids": []
        }

    if plan["route"] == ROUTE_HYBRID:
        prompt = build_hybrid_prompt(question, sql_answer, docs, metas)
    else:
        prompt = build_prompt(question, docs, metas)

    try:
        response = client.models.generate_content(
            model=ANALYTICS_MODEL,
            contents=prompt
        )
        answer = response.text
    except Exception as e:
        if plan["route"] == ROUTE_HYBRID and sql_answer:
            answer = sql_answer + f"\n\nRetrieval note:\n- Retrieved examples were found, but hybrid generation failed: {e}"
        else:
            raise

    return {
        "answer": answer,
        "route": plan["route"],
        "plan": plan,
        "sql_result": sql_result,
        "docs": docs,
        "metas": metas,
        "ids": ids,
        "show_sql": show_sql,
    }


# ── CLI ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="JobScope UK RAG pipeline")
    parser.add_argument("--index", action="store_true", help="Index jobs into ChromaDB")
    parser.add_argument("--reset", action="store_true", help="Reset collection before indexing")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of jobs for testing")
    parser.add_argument("--ask", type=str, help="Ask a grounded question")
    parser.add_argument("--role", type=str, default=None, help="Optional role category filter")
    parser.add_argument("--region", type=str, default=None, help="Optional location region filter")
    parser.add_argument("--seniority", type=str, default=None, help="Optional seniority filter")
    parser.add_argument("--n_results", type=int, default=5, help="Number of retrieved docs")
    parser.add_argument("--source", type=str, default=None, help="Optional source filter, e.g. reed")
    parser.add_argument("--status", action="store_true", help="Show Chroma collection status")
    parser.add_argument("--inspect", action="store_true", help="Inspect a sample of indexed docs")
    parser.add_argument("--preview", type=str, help="Preview retrieved documents for a question")
    parser.add_argument("--show-sql", action="store_true", help="Show SQL query details for analytics answers")



    args = parser.parse_args()

    if args.index:
        index_jobs(
                limit=args.limit,
                reset=args.reset,
                batch_size=100,
                source_only=args.source
            )
    if args.preview:

        preview_retrieval(
        question=args.preview,
        n_results=args.n_results,
        role=args.role,
        region=args.region,
        seniority=args.seniority
    )
    
    if args.inspect:
        inspect_collection_sample()
    
    if args.status:
        show_collection_status()
       
    if args.ask:
        result = ask(
            question=args.ask,
            n_results=args.n_results,
            role=args.role,
            region=args.region,
            seniority=args.seniority,
            show_sql=args.show_sql
        )

        print("\n" + "=" * 80)
        print("ANSWER")
        print("=" * 80)
        print(result["answer"])

        print("\n" + "=" * 80)
        print("ROUTE")
        print("=" * 80)
        print(result.get("route", "rag"))

        sql_result = result.get("sql_result")
        if args.show_sql and sql_result:
            print("\n" + "=" * 80)
            print("SQL DEBUG")
            print("=" * 80)
            print(sql_result.get("sql", ""))
            print("Params:", sql_result.get("params", []))

        if result["metas"]:
            print("\n" + "=" * 80)
            print("RETRIEVED SOURCES")
            print("=" * 80)
            for i, meta in enumerate(result["metas"], start=1):
                print(
                    f"{i}. role={meta.get('role_category')} | "
                    f"region={meta.get('location_region')} | "
                    f"seniority={meta.get('seniority')} | "
                    f"company={meta.get('company')}"
                )


if __name__ == "__main__":
    main()
