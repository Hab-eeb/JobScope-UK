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
from typing import List, Dict, Any, Optional, Tuple

import chromadb
from dotenv import load_dotenv
from google import genai

load_dotenv()

DB_NAME = os.getenv("DB_NAME", "jobscope.db")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
CHROMA_PATH = os.getenv("CHROMA_PATH", "./data/chroma_db")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "uk_jobs")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY is not set in the environment.")

client = genai.Client(api_key=GEMINI_API_KEY)
chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = chroma_client.get_or_create_collection(
    name=COLLECTION_NAME,
    metadata={"hnsw:space": "cosine"}
)

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
    query_embedding = get_embedding(query)
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


def ask(
    question: str,
    n_results: int = 5,
    role: Optional[str] = None,
    region: Optional[str] = None,
    seniority: Optional[str] = None
) -> Dict[str, Any]:
    """Retrieve relevant docs and generate a grounded answer."""
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
        return {
            "answer": "No relevant job postings were retrieved for this query.",
            "docs": [],
            "metas": [],
            "ids": []
        }

    prompt = build_prompt(question, docs, metas)

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    return {
        "answer": response.text,
        "docs": docs,
        "metas": metas,
        "ids": ids
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
            seniority=args.seniority
        )

        print("\n" + "=" * 80)
        print("ANSWER")
        print("=" * 80)
        print(result["answer"])

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