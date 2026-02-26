"""
Pydantic models for strict data validation.
Ensures all job data conforms to expected schema.
"""

import hashlib
from datetime import datetime
from enum import Enum
from typing import Optional, List, Any
from pydantic import BaseModel, Field, field_validator, computed_field


class JobStatus(str, Enum):
    """Job processing status"""
    DETECTED = "detected"
    FILTERED = "filtered"  # Failed LLM/relevance filter
    NOTIFIED = "notified"
    APPLIED = "applied"
    REJECTED = "rejected"
    EXPIRED = "expired"


class JobSource(str, Enum):
    """Known job sources"""
    REMOTEOK = "RemoteOK"
    ARBEITNOW = "Arbeitnow"
    FINDWORK = "Findwork"
    HIMALAYAS = "Himalayas"
    JOBICY = "Jobicy"
    THEMUSE = "TheMuse"
    WEWORKREMOTELY = "WWRemotely"
    INDEED_INDIA = "Indeed-India"
    GOOGLE_JOBS = "Google-Jobs"
    NAUKRI = "Naukri"
    LINKEDIN = "LinkedIn"
    GLASSDOOR = "Glassdoor"
    INSTAHYRE = "Instahyre"
    FOUNDIT = "Foundit"
    HIRIST = "Hirist"
    CUTSHORT = "Cutshort"
    ANGELLIST = "AngelList"
    HN_HIRING = "HN-Hiring"
    LANDINGJOBS = "LandingJobs"
    COMPANY_DIRECT = "Company-Direct"
    OTHER = "Other"


class JobCategory(str, Enum):
    """Job category for matching"""
    DATA_SCIENCE = "Data Science"
    DATA_ANALYTICS = "Data Analytics"
    ML_ENGINEERING = "ML Engineering"
    BI_DEVELOPER = "BI Developer"
    DATA_ENGINEERING = "Data Engineering"
    OTHER = "Other"


class RawJob(BaseModel):
    """
    Raw job data from sources - minimal validation.
    Used for initial ingestion before processing.
    """
    title: str
    company: str = "Unknown"
    location: str = "Unknown"
    description: str = ""
    url: str
    source: str
    posted: Optional[str] = None
    salary: Optional[str] = None
    job_type: Optional[str] = None  # full-time, contract, etc.
    
    @field_validator('posted', mode='before')
    @classmethod
    def convert_posted(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        return str(v)
    
    @field_validator('job_type', mode='before')
    @classmethod
    def convert_job_type(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, list):
            return ', '.join(str(x) for x in v)
        return str(v)
    
    # Metadata
    raw_data: Optional[dict] = None  # Original API response
    fetched_at: datetime = Field(default_factory=datetime.now)
    
    @field_validator('title', 'company', mode='before')
    @classmethod
    def clean_string(cls, v: Any) -> str:
        if v is None:
            return "Unknown"
        return str(v).strip()[:200]
    
    @field_validator('description', mode='before')
    @classmethod
    def clean_description(cls, v: Any) -> str:
        if v is None:
            return ""
        # Remove HTML tags
        import re
        text = str(v)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()[:5000]
    
    @field_validator('url', mode='before')
    @classmethod
    def clean_url(cls, v: Any) -> str:
        if not v:
            return ""
        return str(v).strip()
    
    @computed_field
    @property
    def job_id(self) -> str:
        """Generate unique ID from title + company + source"""
        text = f"{self.title}_{self.company}_{self.source}".lower()
        return hashlib.md5(text.encode()).hexdigest()[:16]


class ProcessedJob(BaseModel):
    """
    Fully processed job with scores and status.
    Ready for database storage.
    """
    # Core fields from RawJob
    job_id: str
    title: str
    company: str
    location: str
    description: str
    url: str
    source: str
    posted: Optional[str] = None
    salary: Optional[str] = None
    job_type: Optional[str] = None
    
    # Processing results
    status: JobStatus = JobStatus.DETECTED
    category: JobCategory = JobCategory.OTHER
    
    # Matching scores (0.0 - 1.0)
    semantic_score: float = 0.0
    keyword_score: float = 0.0
    combined_score: float = 0.0
    
    # LLM analysis results
    llm_suitable: Optional[bool] = None
    llm_experience_required: Optional[str] = None
    llm_reason: Optional[str] = None
    
    # Location analysis
    is_india: bool = False
    is_remote: bool = False
    city: Optional[str] = None
    
    # Timestamps
    fetched_at: datetime = Field(default_factory=datetime.now)
    processed_at: Optional[datetime] = None
    notified_at: Optional[datetime] = None
    applied_at: Optional[datetime] = None
    
    # Job age in hours (calculated)
    age_hours: float = 0.0
    
    @computed_field
    @property
    def is_relevant(self) -> bool:
        """Check if job passes all relevance filters"""
        return (
            self.combined_score >= 0.35 and
            self.is_india and
            (self.llm_suitable is None or self.llm_suitable)
        )


class JobBatch(BaseModel):
    """Batch of jobs from a single source"""
    source: str
    jobs: List[RawJob] = []
    fetched_at: datetime = Field(default_factory=datetime.now)
    fetch_duration_ms: int = 0
    error: Optional[str] = None
    
    @property
    def count(self) -> int:
        return len(self.jobs)


class MatchResult(BaseModel):
    """Result from semantic matching"""
    job_id: str
    semantic_score: float
    keyword_score: float
    combined_score: float
    matched_keywords: List[str] = []
    category: JobCategory = JobCategory.OTHER


class LLMFilterResult(BaseModel):
    """Result from LLM filtering"""
    job_id: str
    suitable: bool
    experience_required: Optional[str] = None
    reason: str = ""
    confidence: float = 0.0


class NotificationPayload(BaseModel):
    """Telegram notification content"""
    job: ProcessedJob
    message: str
    sent_at: Optional[datetime] = None
    success: bool = False


class ScanStats(BaseModel):
    """Statistics from a scan run"""
    started_at: datetime = Field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    
    # Counts
    total_fetched: int = 0
    total_new: int = 0
    total_matched: int = 0
    total_filtered_location: int = 0
    total_filtered_relevance: int = 0
    total_filtered_llm: int = 0
    total_notified: int = 0
    
    # Scores
    best_score: float = 0.0
    avg_score: float = 0.0
    
    # Source breakdown
    source_counts: dict = {}
    
    # Errors
    errors: List[str] = []
    
    @property
    def duration_seconds(self) -> float:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0
