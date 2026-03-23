"""
JobScope UK — Skill Taxonomy
Curated skill list for UK data & AI roles, organised by category.
Used by data_processor.py for rule-based skill extraction from job descriptions.
"""

SKILL_TAXONOMY = {
    "languages": [
        "python", "r", "sql", "java", "scala", "javascript",
        "typescript", "go", "rust", "c++", "julia", "bash", "shell",
        "matlab", "sas", "stata",
    ],
    "ml_frameworks": [
        "tensorflow", "pytorch", "scikit-learn", "keras", "xgboost",
        "lightgbm", "catboost", "huggingface", "transformers",
        "spacy", "nltk", "opencv", "fastai",
    ],
    "llm_and_genai": [
        "langchain", "llamaindex", "openai", "gpt", "gemini",
        "bedrock", "claude", "llama", "mistral", "rag",
        "retrieval augmented generation", "prompt engineering",
        "fine-tuning", "vector database", "embeddings",
        "large language model",
    ],
    "cloud_platforms": [
        "aws", "azure", "gcp", "google cloud",
    ],
    "cloud_services": [
        "sagemaker", "databricks", "snowflake", "bigquery",
        "redshift", "glue", "athena", "lambda", "ec2", "s3",
        "azure ml", "vertex ai", "emr",
    ],
    "data_engineering": [
        "spark", "pyspark", "airflow", "dbt", "kafka",
        "hadoop", "hive", "etl", "data pipeline", "data warehouse",
        "data lake", "data modelling", "data modeling",
        "data governance", "data quality",
    ],
    "databases": [
        "postgresql", "mysql", "mongodb", "redis",
        "elasticsearch", "cassandra", "dynamodb",
        "sqlite", "neo4j", "oracle",
    ],
    "bi_and_visualisation": [
        "tableau", "power bi", "looker", "quicksight",
        "qlik", "qliksense", "qlikview", "metabase",
        "matplotlib", "seaborn", "plotly", "d3",
    ],
    "devops_and_tools": [
        "docker", "kubernetes", "terraform", "jenkins",
        "ci/cd", "github actions", "mlflow", "kubeflow",
        "wandb", "dvc", "git", "linux",
    ],
    "techniques": [
        "machine learning", "deep learning", "reinforcement learning",
        "natural language processing", "nlp", "computer vision",
        "time series", "forecasting", "recommendation systems",
        "anomaly detection", "classification", "regression",
        "clustering", "dimensionality reduction",
        "feature engineering", "model deployment",
        "a/b testing", "hypothesis testing",
        "bayesian", "neural network", "cnn", "rnn", "lstm",
        "transformer", "gan", "automl",
        "statistical modelling", "statistical modeling",
        "statistical analysis", "statistics",
        "predictive modelling", "predictive modeling",
        "data mining", "web scraping", "sentiment analysis",
        "data analysis", "data analytics",
        "data visualisation", "data visualization",
        "data modelling", "data modeling",
        "reporting", "dashboards",
    ],
    "data_formats_and_tools": [
        "pandas", "numpy", "scipy", "polars",
        "excel", "csv", "json", "parquet", "api",
        "rest api", "graphql", "websocket",
    ],
    "soft_skills": [
        "stakeholder management", "communication",
        "agile", "scrum", "jira", "confluence",
        "cross-functional", "leadership", "mentoring",
        "project management", "product management",
    ],
}

# Flatten for quick lookup
ALL_SKILLS = []
SKILL_TO_CATEGORY = {}
for category, skills in SKILL_TAXONOMY.items():
    for skill in skills:
        ALL_SKILLS.append(skill)
        SKILL_TO_CATEGORY[skill] = category
