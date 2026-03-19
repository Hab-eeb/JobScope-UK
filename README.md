# 🔍 JobScope UK

## UK Job Market Intelligence Tool for Data & AI Roles

A data pipeline and analysis tool that collects UK job market data from multiple APIs, extracts skill demand patterns, analyses salary trends, and provides a RAG-powered natural language interface for querying real job market insights.

**Work in progress** — actively being built.

### What It Does

- Collects job postings from **Adzuna** and **Reed** APIs across 8 data/AI role categories
- Cleans and normalises job data with NLP-based skill extraction
- Produces analytical insights on skill demand, salary trends, and regional patterns
- Provides a RAG pipeline for natural language queries grounded in real job data

### Data Collected

| Source | Jobs | Role Categories |
|--------|------|-----------------|
| Adzuna | 1,506 | Data Scientist, Data Analyst, ML Engineer, Data Engineer, AI Engineer, BI Analyst, LLM Engineer, NLP Engineer |
| Reed | 1,259 | Same categories |
| **Total** | **2,765** | |

### Tech Stack

- **Python** — core language
- **Adzuna API + Reed API** — multi-source job data collection
- **SQLite** — relational persistence (raw → clean two-stage pipeline)
- **Pandas, Matplotlib, Seaborn** — analysis and visualisation
- **Google Gemini** — embeddings + generation (RAG pipeline)
- **ChromaDB** — vector store for semantic search

### Project Structure

```
jobscope-uk/
├── data_collector.py     # Multi-source API data collection (Adzuna + Reed)
├── database.py           # SQLite schema + helpers
├── data_processor.py     # Cleaning, normalisation, skill extraction (WIP)
├── skill_taxonomy.py     # Curated skill taxonomy for data roles (WIP)
├── rag_pipeline.py       # RAG: embed, index, retrieve, generate (WIP)
├── notebooks/
│   ├── analysis.ipynb    # EDA + visualisations (WIP)
│   └── rag_demo.ipynb    # RAG demo with example queries (WIP)
├── requirements.txt
├── .env.example
└── README.md
```

### Setup

```bash
git clone https://github.com/Hab-eeb/jobscope-uk.git
cd jobscope-uk
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # Add your API keys
python data_collector.py --max-pages 5
```

### Status

- [x] Multi-source data collection (Adzuna + Reed)
- [x] Deduplication and persistent storage
- [x] Data cleaning and skill extraction
- [ ] Exploratory data analysis
- [ ] RAG pipeline
- [ ] Streamlit demo (stretch goal)
