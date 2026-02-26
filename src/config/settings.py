"""
Configuration settings using Pydantic for validation.
All settings are loaded from environment variables with sensible defaults.
"""

import os
from pathlib import Path
from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load .env file from project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")


class TelegramSettings(BaseSettings):
    """Telegram bot configuration"""
    token: Optional[str] = Field(default=None, alias="TELEGRAM_TOKEN")
    chat_id: Optional[str] = Field(default=None, alias="TELEGRAM_CHAT_ID")
    
    @property
    def is_configured(self) -> bool:
        return bool(self.token and self.chat_id)


class DatabaseSettings(BaseSettings):
    """Database configuration"""
    db_path: Path = Field(default=PROJECT_ROOT / "data" / "jobs.db")
    
    model_config = {"env_prefix": "DB_", "extra": "ignore"}
    

class MatchingSettings(BaseSettings):
    """Semantic matching configuration"""
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    min_semantic_score: float = 0.25  # Minimum cosine similarity (0-1) - lower for broader matches
    cache_embeddings: bool = True
    

class LLMSettings(BaseSettings):
    """LLM filtering configuration"""
    provider: str = Field(default="groq", description="groq, ollama, or openai")
    groq_api_key: Optional[str] = Field(default=None, alias="GROQ_API_KEY")
    ollama_model: str = "llama3.2"
    ollama_url: str = "http://localhost:11434"
    openai_api_key: Optional[str] = Field(default=None, alias="OPENAI_API_KEY")
    enabled: bool = True
    max_experience_years: int = 5  # Filter out jobs requiring more experience
    

class SerpAPISettings(BaseSettings):
    """SerpAPI configuration for Google Jobs"""
    api_key: Optional[str] = Field(default=None, alias="SERPAPI_KEY")
    monthly_limit: int = 250
    usage_file: Path = Field(default=PROJECT_ROOT / "data" / "serpapi_usage.json")
    run_hours: List[int] = [8, 12, 18, 22]  # Hours to use SerpAPI


class JobSearchSettings(BaseSettings):
    """Job search preferences - INDIA focused"""
    
    # Target locations (prioritized)
    target_locations: List[str] = [
        "pune", "mumbai", "bangalore", "bengaluru", "hyderabad",
        "chennai", "delhi", "ncr", "noida", "gurgaon", "gurugram",
        "india", "remote"
    ]
    
    # Location keywords to EXCLUDE (non-India jobs)
    exclude_locations: List[str] = [
        "usa", "u.s.", "united states", "america", "canada", 
        "uk", "united kingdom", "london", "europe", "germany", 
        "australia", "singapore", "dubai", "uae", "philippines",
        "vietnam", "poland", "romania", "brazil", "mexico"
    ]
    
    # Job titles to EXCLUDE (irrelevant roles)
    exclude_titles: List[str] = [
        "sales", "business development", "bdr", "sdr", "account executive",
        "account manager", "customer success", "support engineer",
        "recruiter", "hr", "human resource", "marketing manager",
        "content writer", "copywriter", "graphic designer",
        "civil engineer", "mechanical engineer", "electrical engineer",
        "doctor", "nurse", "teacher", "professor", "chef", "driver"
    ]
    
    # Maximum job age in hours (only notify for jobs newer than this)
    # Set higher for job boards with unreliable timestamps
    max_job_age_hours: int = 168  # 7 days - many job boards are weekly updated
    
    # Concurrent requests limit
    max_concurrent_requests: int = 10
    
    # Request timeout in seconds
    request_timeout: int = 30


class ProfileConfig(BaseSettings):
    """Resume/Profile configuration for matching"""
    
    data_science_profile: str = """
    Data Scientist with expertise in Machine Learning, Deep Learning, and NLP.
    Skills: Python, PyTorch, TensorFlow, Keras, Scikit-learn, XGBoost, LightGBM.
    Experience with: Neural Networks, Transformers, BERT, GPT, LLMs, RAG.
    Statistical Analysis, A/B Testing, Hypothesis Testing, Time Series Forecasting.
    Data Engineering: SQL, PostgreSQL, MongoDB, ETL Pipelines, Airflow, Spark.
    Cloud: AWS (S3, EC2, Lambda, SageMaker), Azure ML, GCP.
    MLOps: Docker, MLflow, Model Deployment, API Development, FastAPI.
    Domain: Predictive Modeling, Recommendation Systems, Computer Vision, NLP.
    Tools: Jupyter, Git, Linux, Pandas, NumPy, Matplotlib, Seaborn.
    """
    
    data_analyst_profile: str = """
    Data Analyst skilled in SQL, Python, and Business Intelligence tools.
    Advanced SQL: Complex Joins, Window Functions, CTEs, Query Optimization.
    Python: Pandas, NumPy, Data Wrangling, Data Cleaning, Automation.
    Visualization: Power BI, Tableau, DAX, Power Query, Advanced Excel.
    Statistics: Descriptive/Inferential Statistics, A/B Testing, Regression.
    Database: MySQL, PostgreSQL, SQL Server, BigQuery, Snowflake.
    ETL: Data Pipelines, Data Integration, Azure Data Factory, dbt.
    Domain: KPI Analysis, Business Insights, Dashboard Development, Reporting.
    Stakeholder Communication, Cross-functional Collaboration.
    """
    
    # Keywords that MUST match for relevance
    core_keywords: List[str] = [
        "data scientist", "data analyst", "machine learning", "ml engineer",
        "data science", "analytics", "python", "sql", "power bi", "tableau",
        "deep learning", "nlp", "ai engineer", "business intelligence",
        "data engineer", "bi developer", "statistician", "quantitative"
    ]


class Settings(BaseSettings):
    """Main settings aggregator"""
    telegram: TelegramSettings = TelegramSettings()
    database: DatabaseSettings = DatabaseSettings()
    matching: MatchingSettings = MatchingSettings()
    llm: LLMSettings = LLMSettings()
    serpapi: SerpAPISettings = SerpAPISettings()
    search: JobSearchSettings = JobSearchSettings()
    profile: ProfileConfig = ProfileConfig()
    
    # Logging
    log_level: str = "INFO"
    log_file: Path = Field(default=PROJECT_ROOT / "data" / "watchdog.log")
    
    class Config:
        env_file = ".env"
        extra = "ignore"


# Global settings instance
settings = Settings()

# Ensure data directory exists
(PROJECT_ROOT / "data").mkdir(exist_ok=True)
