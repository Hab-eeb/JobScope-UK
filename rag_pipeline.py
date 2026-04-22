"""
JobScope UK — RAG Pipeline

Phase 4:
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
from typing import List, Dict, Any, Optional

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


# ── Database Loading ───────────────────────────────────────────────────

def load_clean_jobs(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Load cleaned jobs from SQLite."""
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

    if limit:
        query += f" LIMIT {int(limit)}"

    rows = conn.execute(query).fetchall()
    conn.close()

    jobs = []
    for row in rows:
        job = dict(row)

        # Parse extracted skills safely
        try:
            job["extracted_skills"] = json.loads(job["extracted_skills"]) if job["extracted_skills"] else []
        except json.JSONDecodeError:
            job["extracted_skills"] = []

        jobs.append(job)

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

def get_embedding(text: str) -> List[float]:
    """Generate a Gemini embedding for a single text."""
    response = client.models.embed_content(
        model="gemini-embedding-001",
        contents=text
    )
    return response.embeddings[0].values


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


def index_jobs(limit: Optional[int] = None, reset: bool = False, batch_size: int = 25) -> None:
    """Index jobs into ChromaDB."""
    if reset:
        print("Resetting Chroma collection...")
        reset_collection()

    jobs = load_clean_jobs(limit=limit)
    print(f"Loaded {len(jobs)} cleaned jobs from SQLite.")

    if not jobs:
        print("No jobs found to index.")
        return

    documents = []
    embeddings = []
    ids = []
    metadatas = []

    for i, job in enumerate(jobs, start=1):
        doc_text = create_rag_document(job)

        try:
            embedding = get_embedding(doc_text)
        except Exception as e:
            print(f"Embedding failed for job {job['id']}: {e}")
            continue

        documents.append(doc_text)
        embeddings.append(embedding)
        ids.append(f"job_{job['id']}")
        metadatas.append({
            "job_id": job["id"],
            "role_category": job.get("role_category", "Unknown"),
            "location_region": job.get("location_region", "Unknown"),
            "seniority": job.get("seniority", "Unknown"),
            "company": job.get("company", "Unknown"),
            "has_real_salary": int(job.get("has_real_salary", 0) or 0),
            "salary_mid": float(job["salary_mid"]) if job.get("salary_mid") is not None else 0.0,
        })

        # Small delay to be gentle with rate limits
        time.sleep(0.1)

        if len(documents) >= batch_size:
            collection.add(
                documents=documents,
                embeddings=embeddings,
                ids=ids,
                metadatas=metadatas
            )
            print(f"Indexed {i}/{len(jobs)} jobs...")
            documents, embeddings, ids, metadatas = [], [], [], []

    # Add leftovers
    if documents:
        collection.add(
            documents=documents,
            embeddings=embeddings,
            ids=ids,
            metadatas=metadatas
        )

    print("Indexing complete.")


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

    args = parser.parse_args()

    if args.index:
        index_jobs(limit=args.limit, reset=args.reset)

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