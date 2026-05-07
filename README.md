# 🔍 JobScope UK

## UK Job Market Intelligence Tool for Data & AI Roles

JobScope UK is a portfolio project that analyzes the UK job market for data and AI roles using real job postings collected from the Adzuna and Reed APIs. It combines a structured data pipeline, exploratory analysis, NLP-based skill extraction, and a SQL-aware Retrieval-Augmented Generation (RAG) interface that lets users ask both qualitative and analytical questions over the dataset.

The project was designed to answer a practical question: **what skills, tools, and role patterns are employers in the UK really asking for across data and AI jobs?**

---

## Why I Built This

As someone applying into the UK data and AI market, I wanted to build a project around the market I am actually hiring into. Rather than relying on generic career advice, I wanted to analyze real postings, extract recurring skill patterns, compare role categories, and then build a grounded question-answering interface on top of the results.

This project demonstrates:

- data collection from external APIs
- data cleaning and normalization
- NLP / rule-based skill extraction
- EDA and storytelling with visualizations
- RAG architecture using Gemini + ChromaDB
- practical tradeoff handling around source quality, embeddings, retrieval, and analytical routing

---

## Dataset Summary

After cleaning and processing, the project produced:

- **2,368 cleaned job postings**
- **8 role categories**
- **35% salary coverage** with real salary data
- postings grouped by role category, seniority, region, extracted skills, and salary information where available

### Jobs by role category

- AI Engineer: 682
- Data Analyst: 561
- Data Scientist: 341
- Data Engineer: 329
- ML Engineer: 257
- BI Analyst: 158
- LLM Engineer: 27
- NLP Engineer: 13

### Jobs by seniority

- Mid: 1,251
- Junior: 592
- Senior: 525

### Top regions

- London: 1,025
- Other UK: 277
- UK-wide: 173
- North West: 157
- Yorkshire: 133
- West Midlands: 124
- South East: 122

---

## Tech Stack

- **Python** — core language
- **Adzuna API + Reed API** — data collection
- **SQLite** — raw and cleaned storage
- **Pandas** — transformation and analysis
- **Matplotlib / Seaborn** — visualization
- **Google Gemini** — embeddings and answer generation
- **ChromaDB** — vector store for semantic retrieval
- **Jupyter Notebook** — exploratory analysis
- **Streamlit** — optional app layer for interactive demo

---

## Architecture

The project is split into four main layers:

### 1. Data Collection
Job postings are collected from the Adzuna and Reed APIs across the target role categories and stored in a raw SQLite table.

### 2. Data Processing
Raw jobs are cleaned, normalized, deduplicated, and enriched. This includes:
- title normalization
- seniority inference
- location extraction
- salary midpoint calculation
- rule-based skill extraction using a curated taxonomy

### 3. Analysis
The cleaned dataset is analyzed to identify:
- role distribution
- regional concentration
- skill demand patterns
- salary variation
- skill co-occurrence
- differences across role categories

### 4. SQL-Aware RAG Pipeline
The query layer now supports three answer paths:
- **RAG** for qualitative, posting-grounded questions
- **SQL** for representative dataset-wide analysis
- **Hybrid** for questions that need both aggregate evidence and example postings

A subset of high-quality job postings is embedded and indexed into ChromaDB, while the cleaned SQLite dataset supports structured analytical queries over skills, salaries, role counts, seniority, source, and region.

---

## Repository Structure

```text
jobscope-uk/
├── data_collector.py
├── database.py
├── data_processor.py
├── fetch_full_descriptions.py
├── skill_taxonomy.py
├── rag_pipeline.py
├── analysis.ipynb
├── requirements.txt
├── env_example.txt
├── outputs/
│   ├── role_distribution.png
│   ├── seniority_distribution.png
│   ├── regional_distribution.png
│   ├── top_skills.png
│   ├── skills_by_role_heatmap.png
│   ├── skill_cooccurrence.png
│   ├── source_comparison.png
│   ├── salary_by_role.png
│   ├── salary_by_skill.png
│   ├── regional_skills.png
│   └── role_category_skills.png
└── README.md
```

---

## Key Findings

### 1. Python, machine learning, and SQL form the core technical foundation
Python is the most frequently mentioned technical skill in the dataset, followed closely by machine learning and SQL. Together, they form the strongest shared technical base across multiple data and AI role categories.

### 2. The market is not purely “advanced AI”
Although AI/ML tools appear strongly, the UK market still shows high demand for business-facing skills such as reporting, dashboards, Excel, Power BI, Tableau, and communication. This suggests that practical analytics remains a major part of the market.

### 3. Role titles have distinct skill fingerprints
Different roles show clear differences in expected tools and capabilities:
- **Data Analyst / BI Analyst** roles skew toward reporting, dashboards, SQL, Excel, Power BI, and Tableau
- **Data Scientist** roles emphasize Python, machine learning, SQL, and modeling libraries
- **Data Engineer** roles lean toward SQL, Python, ETL/ELT, cloud platforms, and data warehousing
- **AI / LLM-oriented roles** show stronger demand for GenAI frameworks, cloud AI services, and deployment tooling

### 4. London remains the main hiring hub
London dominates the dataset by volume and also shows a higher salary distribution than non-London roles in the disclosed-salary subset.

### 5. Mid-level roles dominate the market
The dataset is strongly skewed toward mid-level roles, with fewer junior and senior openings overall.

### 6. Source quality materially affects extraction
Reed has a seperate api that allows for full job descriptions unlike Adzuna descriptions that are capped at 500 characters, so for good skill extraction i had to limit to only reed jobs leading to richer skill extraction and better downstream retrieval quality for the RAG pipeline.

---

## Sample Visualizations

### Role distribution
![Role distribution](outputs/role_distribution.png)

### Regional distribution
![Regional distribution](outputs/regional_distribution.png)

### Top skills overall
![Top skills](outputs/top_skills.png)

### Skills by role category
![Skills by role heatmap](outputs/skills_by_role_heatmap.png)

More charts are available in the [`outputs/`](outputs) folder and the analysis notebook.

---

## SQL-Aware RAG System

The answer layer was built to support natural-language questions over real job postings without forcing every question through retrieval alone.

### How it works

1. Cleaned job postings are stored in SQLite with structured fields such as role, region, seniority, salary, and extracted skills
2. High-quality postings are converted into enriched text documents
3. Gemini embeddings are created for those documents
4. The embeddings and metadata are stored in **ChromaDB**
5. A user question is classified into one of three routes:
   `rag`, `sql`, or `hybrid`
6. The system either:
   - retrieves similar postings from ChromaDB
   - runs a constrained analytical SQL query over `clean_jobs`
   - or combines both into one answer
7. Gemini generates the final answer when the route requires natural-language synthesis

### Routing logic

- **RAG** is used for qualitative questions such as:
  “What do junior data analyst roles usually require?”
- **SQL** is used for aggregate questions such as:
  “What are the top 10 skills for Data Analyst?”
- **Hybrid** is used when the user wants representative analysis plus real examples, such as:
  “What skills do Data Analyst jobs usually mention?”

The SQL layer is intentionally constrained. The pipeline does not execute arbitrary model-written SQL. Instead, it maps supported question types into approved query templates for:
- top skills by role / region / seniority
- job counts and grouped breakdowns
- salary summaries and median salary by group
- skill demand comparisons across roles or other groupings

### Why ChromaDB
ChromaDB was used as the vector database because it provides:
- persistent local storage
- semantic similarity search
- metadata filtering
- a simple Python API for prototyping

### Practical note
Indexing focused on **Reed postings** because they contained fuller descriptions than Adzuna postings, which led to better retrieval quality. SQLite remains the analytical source of truth for representative counts and aggregations.

---

## Example Questions and Answers

Below are real captured outputs from the CLI rather.

### 1. RAG example

```bash
./jbvenv/bin/python rag_pipeline.py --ask "What do junior data analyst roles usually require?" --role "Data Analyst" --seniority junior
```

```text
================================================================================
ANSWER
================================================================================
Answer:
Junior data analyst roles, particularly trainee positions, are designed for individuals with limited or no prior experience. These roles typically require a strong aptitude for detail, perceptiveness, organisation, competence, and analytical skills, coupled with good communication abilities. The postings suggest that these roles often involve a structured career program that includes obtaining specific qualifications and training in various data analysis tools and methodologies.

Evidence from postings:
- These roles are specifically designed for entry-level individuals with limited or no experience.
- Key transferable skills sought include being detail-oriented, perceptive, organised, competent, analytical, and having good communication skills.
- The advertised positions involve a career programme that includes obtaining a Comptia Data+ qualification.
- Training in essential data analysis tools such as Microsoft Excel (to expert level), SQL, Python 3, and Tableau is a common requirement.
- Further training in Business Analysis Foundation is often included to enhance employability.

Source summary:
The answer is based on multiple junior Data Analyst (Trainee) roles from ITOL Recruit across various UK regions (West Midlands, Yorkshire, Other UK, South East).

================================================================================
RETRIEVED SOURCES
================================================================================
1. role=Data Analyst | region=West Midlands | seniority=junior | company=ITOL Recruit
2. role=Data Analyst | region=Yorkshire | seniority=junior | company=ITOL Recruit
3. role=Data Analyst | region=Other UK | seniority=junior | company=ITOL Recruit
4. role=Data Analyst | region=Other UK | seniority=junior | company=ITOL Recruit
5. role=Data Analyst | region=South East | seniority=junior | company=ITOL Recruit
```

### 2. SQL example

```bash
./jbvenv/bin/python rag_pipeline.py --ask "What are the top 10 skills for Data Analyst?" --show-sql
```

```text
================================================================================
ANSWER
================================================================================
Answer:
- The top skill for role=Data Analyst is reporting (260 jobs, 46.3% of matching postings).

Result table:
| skill | job_count | pct_jobs |
| --- | --- | --- |
| reporting | 260 | 46.3 |
| sql | 249 | 44.4 |
| dashboards | 240 | 42.8 |
| excel | 197 | 35.1 |
| python | 195 | 34.8 |
| data analysis | 194 | 34.6 |
| tableau | 182 | 32.4 |
| data analytics | 157 | 28.0 |
| power bi | 120 | 21.4 |
| data mining | 108 | 19.3 |

Dataset basis:
- Filters used: role=Data Analyst
- Matching sample size: 561

Notes:
- None

================================================================================
ROUTE
================================================================================
sql
```

### 3. Hybrid example

```bash
./jbvenv/bin/python rag_pipeline.py --ask "What is the demand for r for data analyst and AI engineer and what are the top 4 roles that require r as a skill" --show-sql
```

```text
================================================================================
ANSWER
================================================================================
Answer:
- R is a less prevalent skill for data analyst and AI engineer roles compared to Python, appearing in 17.5% of matching postings.

Dataset-wide evidence:
- Python is the most in-demand skill overall, appearing in 32.6% of the 2368 clean jobs analysed.
- Machine learning is also a highly sought-after skill, present in 29.4% of clean jobs.
- SQL and reporting skills are required in 18.1% and 17.5% of clean jobs, respectively.

Examples from postings:
- One AI Engineer role in the South East explicitly lists 'r' alongside Python and SQL as an extracted skill, indicating its utility in data analysis and model development.
- Another AI Engineer position in London mentions 'r' as a potential skill for building proofs of concept, alongside Python and C#.

================================================================================
ROUTE
================================================================================
hybrid

================================================================================
SQL DEBUG
================================================================================
SELECT
            skill.value AS skill,
            COUNT(*) AS job_count,
            ROUND(100.0 * COUNT(*) / NULLIF(?, 0), 1) AS pct_jobs
        FROM clean_jobs j, json_each(j.extracted_skills) AS skill
        WHERE 1=1
        GROUP BY skill.value
        ORDER BY job_count DESC, skill.value ASC
        LIMIT ?
Params: [2368, 4]
```

---

## Example CLI Usage

### Index jobs into ChromaDB
```bash
./jbvenv/bin/python rag_pipeline.py --index --source reed --limit 100
```

### Ask a question
```bash
./jbvenv/bin/python rag_pipeline.py --ask "What tools are common in data engineer jobs?"
```

### Ask a filtered question
```bash
./jbvenv/bin/python rag_pipeline.py --ask "What do junior data analyst roles usually require?" --role "Data Analyst" --seniority junior
```

### Run an analytical SQL-backed question
```bash
./jbvenv/bin/python rag_pipeline.py --ask "What are the top 10 skills for Data Analyst?" --show-sql
```

### Compare skill demand across roles
```bash
./jbvenv/bin/python rag_pipeline.py --ask "Compare Python demand for Data Analyst vs Data Scientist" --show-sql
```

### Example hybrid-style prompts
```bash
./jbvenv/bin/python rag_pipeline.py --ask "What is the demand for r for data analyst and AI engineer" --show-sql
./jbvenv/bin/python rag_pipeline.py --ask "What is the demand for r for data analyst and AI engineer and what are the top 4 roles that require r as a skill" --show-sql
```

### Show the selected route
When `--show-sql` is enabled, the CLI also shows:
- the chosen route: `rag`, `sql`, or `hybrid`
- the generated SQL template and parameters for analytical queries

---

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/Hab-eeb/jobscope-uk.git
cd jobscope-uk
```

### 2. Create and activate a virtual environment
```bash
python3 -m venv jbvenv
source jbvenv/bin/activate
```

### 3. Install requirements
```bash
./jbvenv/bin/python -m pip install -r requirements.txt
```

### 4. Create environment variables
Create a `.env` file based on `env_example.txt` and add your API keys.

Example:
```env
ADZUNA_APP_ID=your_id
ADZUNA_APP_KEY=your_key
REED_API_KEY=your_key
GEMINI_API_KEY=your_key
DB_NAME=jobscope.db
CHROMA_PATH=./data/chroma_db
COLLECTION_NAME=uk_jobs
```

### 5. Run the pipeline
```bash
python data_collector.py
python data_processor.py
python rag_pipeline.py --index --source reed
python rag_pipeline.py --ask "What skills are common in data scientist roles?"
```

---

## Limitations

- Salary coverage is incomplete, since many postings do not disclose salary
- Adzuna descriptions are often shorter than Reed descriptions, which affects skill extraction quality
- Hybrid answers are only as good as both layers: SQL can be representative, but retrieved examples still depend on embedding quality
- The analytical layer is intentionally constrained to approved question patterns rather than open-ended arbitrary SQL generation
- Ambiguous prompts can still route imperfectly, especially when a question mixes several analytical asks into one sentence

---

## Next Steps

- add a lightweight Streamlit interface
- expand the constrained SQL intents to support more multi-part analytical questions
- improve routing for mixed prompts that combine demand, comparison, and ranking in one query
- improve document representations for even stronger retrieval quality
- expand README demo examples and deployment options

---

## What This Project Demonstrates

This project shows the ability to:
- build a multi-source data pipeline
- clean and normalize messy real-world job data
- extract structured information from unstructured text
- analyze labour-market patterns with narrative-driven EDA
- design and implement a practical SQL-aware RAG pipeline using modern tools
- make grounded engineering tradeoffs around retrieval quality and API limits

---

## Author

**Habeebullah Agbaje**

If you're reviewing this project as a recruiter, hiring manager, or collaborator, I’d be happy to walk through the architecture, the tradeoffs I made, and how I would extend this into a production-grade market intelligence tool.
