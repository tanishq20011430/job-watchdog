"""
Semantic Matching Engine using Sentence Transformers.
Replaces TF-IDF with proper semantic understanding.
"""

import re
from typing import List, Dict, Optional, Tuple
from functools import lru_cache
import logging
import numpy as np

from datetime import datetime

from ..database.models import RawJob, ProcessedJob, MatchResult, JobCategory, JobStatus
from ..config.settings import settings

logger = logging.getLogger(__name__)

# Lazy load sentence transformers
_model = None
_embeddings_cache = {}
_model_failed = False


def reset_model():
    """Reset the model (useful after errors)"""
    global _model, _model_failed
    _model = None
    _model_failed = False


def get_model():
    """Lazy load the sentence transformer model"""
    global _model, _model_failed
    
    if _model_failed:
        return None
    
    if _model is None:
        try:
            from sentence_transformers import SentenceTransformer
            logger.info(f"Loading model: {settings.matching.embedding_model}")
            _model = SentenceTransformer(settings.matching.embedding_model)
            logger.info("Model loaded successfully")
        except ImportError:
            logger.error("sentence-transformers not installed. Run: pip install sentence-transformers")
            _model_failed = True
            return None
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            _model_failed = True
            return None
    return _model


def get_embedding(text: str) -> Optional[np.ndarray]:
    """Get embedding for text, with caching. Returns None on error."""
    global _model, _model_failed
    
    # Normalize text
    text = text.strip().lower()[:5000]  # Limit length
    
    if settings.matching.cache_embeddings:
        cache_key = hash(text)
        if cache_key in _embeddings_cache:
            return _embeddings_cache[cache_key]
    
    model = get_model()
    if model is None:
        return None
    
    try:
        embedding = model.encode(text, convert_to_numpy=True, normalize_embeddings=True)
        
        if settings.matching.cache_embeddings:
            _embeddings_cache[cache_key] = embedding
        
        return embedding
    except Exception as e:
        logger.warning(f"Embedding error: {e}")
        # Try to reload model on next call
        _model = None
        return None


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Calculate cosine similarity between two vectors"""
    # Vectors are already normalized, so dot product = cosine similarity
    return float(np.dot(a, b))


class SemanticMatcher:
    """
    Semantic job matching engine.
    Uses sentence embeddings for semantic understanding.
    """
    
    # Profile embeddings (computed once)
    _profile_embeddings: Dict[str, np.ndarray] = {}
    
    # Keywords that MUST be present for relevance (case-insensitive)
    REQUIRED_KEYWORDS = [
        "data", "analyst", "scientist", "machine learning", "ml", 
        "python", "sql", "analytics", "bi", "intelligence",
        "statistics", "deep learning", "ai", "artificial",
        "engineer", "nlp", "power bi", "tableau", "visualization"
    ]
    
    # Keywords that indicate IRRELEVANT jobs (exclusion filter)
    EXCLUDE_KEYWORDS = [
        # Non-data roles
        "sales", "salesperson", "business development", "bdr", "sdr",
        "account executive", "account manager", "customer success",
        "recruiter", "hr partner", "talent acquisition", "human resource",
        "marketing manager", "content writer", "copywriter",
        "graphic designer", "ui/ux designer", "product designer",
        # Non-tech roles
        "civil engineer", "mechanical engineer", "electrical engineer",
        "doctor", "nurse", "teacher", "professor", "chef", "driver",
        "accountant", "finance manager", "ca", "chartered accountant",
        # Seniority too high
        "director", "vp", "vice president", "chief", "cto", "cdo", "ceo",
        "head of", "principal architect", "distinguished engineer",
    ]
    
    # Location exclusion keywords
    EXCLUDE_LOCATIONS = [
        "usa", "u.s.", "united states", "america", "canada",
        "uk", "united kingdom", "london", "europe", "germany",
        "australia", "singapore", "dubai", "uae", "philippines",
        "vietnam", "poland", "romania", "brazil", "mexico",
        "nigeria", "kenya", "south africa"
    ]
    
    # India location keywords
    INDIA_LOCATIONS = [
        "india", "pune", "mumbai", "bangalore", "bengaluru", 
        "hyderabad", "chennai", "delhi", "ncr", "noida", 
        "gurgaon", "gurugram", "kolkata", "ahmedabad", "jaipur",
        "remote", "work from home", "wfh", "hybrid"
    ]
    
    def __init__(self):
        self._init_profile_embeddings()
    
    def _init_profile_embeddings(self):
        """Pre-compute profile embeddings"""
        if self._profile_embeddings:
            return
        
        try:
            ds_profile = settings.profile.data_science_profile
            da_profile = settings.profile.data_analyst_profile
            
            ds_emb = get_embedding(ds_profile)
            da_emb = get_embedding(da_profile)
            
            if ds_emb is not None:
                self._profile_embeddings["data_science"] = ds_emb
                logger.info("Data Science profile embedding initialized")
            else:
                logger.warning("Data Science embedding failed - using keyword matching only")
            
            if da_emb is not None:
                self._profile_embeddings["data_analytics"] = da_emb
                logger.info("Data Analytics profile embedding initialized")
            else:
                logger.warning("Data Analytics embedding failed - using keyword matching only")
                
        except Exception as e:
            logger.warning(f"Could not initialize embeddings: {e} - using keyword matching only")
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        # Remove HTML
        text = re.sub(r'<[^>]+>', ' ', text)
        # Remove special characters but keep spaces
        text = re.sub(r'[^\w\s\-\+\#\.]', ' ', text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip().lower()
    
    def _check_location_india(self, location: str, description: str = "") -> Tuple[bool, bool, Optional[str]]:
        """
        Check if job is in India or remote-friendly.
        Returns: (is_india, is_remote, city)
        
        Strategy: Be more lenient to avoid false negatives
        - If no location info, assume potentially accessible
        - Remote jobs without explicit country restrictions are accessible
        """
        combined = f"{location} {description}".lower()
        
        # Check for remote keywords
        remote_keywords = ["remote", "work from home", "wfh", "anywhere", "worldwide", 
                          "global", "distributed", "fully remote", "remote-first", "apac"]
        is_remote = any(r in combined for r in remote_keywords)
        
        # Check for explicit India locations
        india_explicit = any(loc in combined for loc in self.INDIA_LOCATIONS[:11])  # India + cities
        
        # Check for excluded locations (US-only, Europe-only etc.)
        exclusion_patterns = [
            "us only", "usa only", "united states only",
            "europe only", "eu only", "uk only",
            "us-based", "us based", "must be in us",
            "canada only", "australia only"
        ]
        has_exclusion = any(excl in combined for excl in exclusion_patterns)
        
        # Check for explicit non-India locations without remote
        has_explicit_other = any(excl in combined for excl in self.EXCLUDE_LOCATIONS)
        
        # Determine if accessible from India:
        # 1. Explicitly mentions India -> Yes
        # 2. Remote/Global with no exclusion -> Yes
        # 3. No location info at all -> Assume Yes (better to include than exclude)
        # 4. Explicit other country without remote -> No
        
        location_clean = location.strip().lower()
        no_location_info = location_clean in ["", "unknown", "n/a", "not specified", "tbd"]
        
        if india_explicit:
            is_india = True
        elif is_remote and not has_exclusion:
            is_india = True  # Remote job accessible from India
        elif no_location_info:
            is_india = True  # No location mentioned, assume accessible
        elif has_explicit_other and not is_remote:
            is_india = False  # Specific non-India location, no remote option
        else:
            # Default to True for remote-friendly check
            is_india = is_remote or not has_explicit_other
        
        # Extract city
        city = None
        city_map = {
            "pune": "Pune",
            "mumbai": "Mumbai",
            "bangalore": "Bangalore",
            "bengaluru": "Bangalore",
            "hyderabad": "Hyderabad",
            "chennai": "Chennai",
            "delhi": "Delhi",
            "noida": "Noida",
            "gurgaon": "Gurgaon",
            "gurugram": "Gurgaon",
            "kolkata": "Kolkata",
        }
        for loc, city_name in city_map.items():
            if loc in combined:
                city = city_name
                break
        
        return is_india, is_remote, city
    
    def _check_title_relevance(self, title: str) -> Tuple[bool, float]:
        """
        Check if job title is relevant.
        Returns: (is_relevant, penalty_score)
        """
        title_lower = title.lower()
        
        # Check for exclusion keywords
        for excl in self.EXCLUDE_KEYWORDS:
            if excl in title_lower:
                return False, -0.5
        
        # Check for required keywords in title
        has_required = any(kw in title_lower for kw in [
            "data", "analyst", "scientist", "ml", "machine learning",
            "ai", "intelligence", "analytics", "bi", "engineer", "python"
        ])
        
        if not has_required:
            return False, -0.3
        
        return True, 0.0
    
    def _calculate_keyword_score(self, text: str) -> float:
        """
        Calculate keyword-based score (0-1).
        Checks for presence of relevant skills.
        """
        text_lower = text.lower()
        
        # Weighted keywords
        keywords = {
            # High value (0.1 each)
            "data scientist": 0.1, "data analyst": 0.1, "machine learning": 0.1,
            "deep learning": 0.1, "ml engineer": 0.1, "nlp": 0.08,
            "power bi": 0.08, "tableau": 0.08, "pytorch": 0.08,
            "tensorflow": 0.08, "scikit-learn": 0.07, "xgboost": 0.07,
            
            # Medium value (0.05 each)
            "python": 0.05, "sql": 0.05, "pandas": 0.05, "numpy": 0.05,
            "statistics": 0.05, "regression": 0.05, "classification": 0.05,
            "neural network": 0.05, "transformer": 0.05, "bert": 0.05,
            "gpt": 0.05, "llm": 0.05, "rag": 0.05,
            "airflow": 0.05, "spark": 0.05, "aws": 0.05, "azure": 0.05,
            "docker": 0.05, "mlops": 0.05, "model deployment": 0.05,
            
            # Lower value (0.03 each)
            "excel": 0.03, "dashboard": 0.03, "visualization": 0.03,
            "etl": 0.03, "data pipeline": 0.03, "jupyter": 0.03,
            "git": 0.03, "api": 0.03, "fastapi": 0.03,
        }
        
        score = 0.0
        matched = []
        
        for keyword, weight in keywords.items():
            if keyword in text_lower:
                score += weight
                matched.append(keyword)
        
        return min(score, 1.0)  # Cap at 1.0
    
    def _determine_category(self, title: str, description: str) -> JobCategory:
        """Determine job category based on content"""
        text = f"{title} {description}".lower()
        
        # Check patterns
        if any(x in text for x in ["ml engineer", "machine learning engineer", "deep learning"]):
            return JobCategory.ML_ENGINEERING
        elif any(x in text for x in ["data scientist", "research scientist", "applied scientist"]):
            return JobCategory.DATA_SCIENCE
        elif any(x in text for x in ["data engineer", "etl", "data pipeline", "spark", "airflow"]):
            return JobCategory.DATA_ENGINEERING
        elif any(x in text for x in ["power bi", "tableau", "bi developer", "business intelligence"]):
            return JobCategory.BI_DEVELOPER
        elif any(x in text for x in ["data analyst", "business analyst", "analytics"]):
            return JobCategory.DATA_ANALYTICS
        
        return JobCategory.OTHER
    
    def _parse_job_age(self, posted: str) -> float:
        """
        Parse posted time and return age in hours.
        
        Strategy: Be lenient - if we can't parse, assume fresh.
        Many job boards don't provide reliable timestamps.
        """
        if not posted:
            return 24  # No timestamp = assume 1 day old (within limit)
        
        now = datetime.now()
        posted_lower = str(posted).lower().strip()
        
        # Handle common relative time patterns
        if any(x in posted_lower for x in ['just', 'now', 'moment', 'second', 'few']):
            return 0
        
        if 'minute' in posted_lower:
            try:
                mins = int(''.join(filter(str.isdigit, posted_lower.split('minute')[0])) or '0')
                return mins / 60
            except:
                return 0.5
        
        if 'hour' in posted_lower:
            try:
                hours = int(''.join(filter(str.isdigit, posted_lower.split('hour')[0])) or '0')
                return hours
            except:
                return 5
        
        if 'today' in posted_lower:
            return 6
        
        if 'yesterday' in posted_lower:
            return 30  # ~30 hours ago
        
        if 'day' in posted_lower:
            try:
                days = int(''.join(filter(str.isdigit, posted_lower.split('day')[0])) or '0')
                return days * 24
            except:
                return 48  # Unknown days = assume 2 days
        
        if 'week' in posted_lower:
            try:
                weeks = int(''.join(filter(str.isdigit, posted_lower.split('week')[0])) or '1')
                return weeks * 7 * 24
            except:
                return 168  # 1 week
        
        if 'month' in posted_lower or 'year' in posted_lower:
            return float('inf')  # Definitely too old
        
        # Try ISO date parsing (e.g., "2026-02-26T10:30:00Z")
        if 'T' in posted or ('-' in posted and len(posted) >= 10):
            try:
                date_part = posted.split('T')[0] if 'T' in posted else posted[:10]
                if '+' in date_part:
                    date_part = date_part.split('+')[0]
                posted_dt = datetime.strptime(date_part, '%Y-%m-%d')
                age_hours = (now - posted_dt).total_seconds() / 3600
                return max(0, age_hours)
            except:
                pass
        
        # Try Unix timestamp (milliseconds)
        if posted.isdigit() and len(posted) >= 10:
            try:
                ts = int(posted)
                if ts > 1000000000000:  # Milliseconds
                    ts = ts / 1000
                posted_dt = datetime.fromtimestamp(ts)
                age_hours = (now - posted_dt).total_seconds() / 3600
                return max(0, age_hours)
            except:
                pass
        
        # Default: unparseable = assume 24h (within limits)
        return 24
    
    def match_job(self, job: RawJob) -> ProcessedJob:
        """
        Process and score a single job.
        Returns ProcessedJob with all scores and flags.
        """
        # Clean text
        title_clean = self._clean_text(job.title)
        desc_clean = self._clean_text(job.description)
        job_text = f"{title_clean} {desc_clean}"
        
        # Calculate job age
        age_hours = self._parse_job_age(job.posted)
        
        # Check title relevance
        is_title_relevant, title_penalty = self._check_title_relevance(job.title)
        
        # Check location
        is_india, is_remote, city = self._check_location_india(job.location, job.description)
        
        # Calculate keyword score
        keyword_score = self._calculate_keyword_score(job_text)
        
        # Calculate semantic score
        semantic_score = 0.0
        if job_text and len(job_text) > 50:
            try:
                job_embedding = get_embedding(job_text)
                
                if job_embedding is not None:
                    # Compare with both profiles, take max
                    ds_embedding = self._profile_embeddings.get("data_science")
                    da_embedding = self._profile_embeddings.get("data_analytics")
                    
                    if ds_embedding is not None:
                        ds_score = cosine_similarity(job_embedding, ds_embedding)
                    else:
                        ds_score = 0.0
                    
                    if da_embedding is not None:
                        da_score = cosine_similarity(job_embedding, da_embedding)
                    else:
                        da_score = 0.0
                    
                    semantic_score = max(ds_score, da_score)
                else:
                    # Fallback to keyword-only matching
                    semantic_score = keyword_score * 0.8  # Use keyword score as proxy
            except Exception as e:
                logger.warning(f"Embedding error: {e}")
                semantic_score = keyword_score * 0.8  # Fallback
        
        # Combined score (weighted average)
        # Semantic: 60%, Keywords: 40%
        combined_score = (semantic_score * 0.6) + (keyword_score * 0.4)
        
        # Apply penalties
        if not is_title_relevant:
            combined_score = max(0, combined_score + title_penalty)
        
        if not is_india:
            combined_score *= 0.3  # Heavy penalty for non-India jobs
        
        # Determine category
        category = self._determine_category(job.title, job.description)
        
        # Determine status
        is_fresh = age_hours <= settings.search.max_job_age_hours
        
        if not is_india:
            status = JobStatus.FILTERED
        elif not is_title_relevant:
            status = JobStatus.FILTERED
        elif not is_fresh:
            status = JobStatus.FILTERED  # Job too old
        elif combined_score < settings.matching.min_semantic_score:
            status = JobStatus.FILTERED
        else:
            status = JobStatus.DETECTED
        
        return ProcessedJob(
            job_id=job.job_id,
            title=job.title,
            company=job.company,
            location=job.location,
            description=job.description[:3000],
            url=job.url,
            source=job.source,
            posted=job.posted,
            salary=job.salary,
            job_type=job.job_type,
            
            status=status,
            category=category,
            
            semantic_score=round(semantic_score, 4),
            keyword_score=round(keyword_score, 4),
            combined_score=round(combined_score, 4),
            
            is_india=is_india,
            is_remote=is_remote,
            city=city,
            age_hours=age_hours,
            
            fetched_at=job.fetched_at,
            processed_at=None,
        )
    
    def match_jobs(self, jobs: List[RawJob]) -> List[ProcessedJob]:
        """Process and score multiple jobs"""
        processed = []
        for job in jobs:
            try:
                processed.append(self.match_job(job))
            except Exception as e:
                logger.warning(f"Error processing job {job.title}: {e}")
                continue
        return processed
    
    def get_top_matches(self, jobs: List[ProcessedJob], limit: int = 20) -> List[ProcessedJob]:
        """Get top matching jobs sorted by score"""
        relevant = [j for j in jobs if j.status == JobStatus.DETECTED and j.is_india]
        relevant.sort(key=lambda x: x.combined_score, reverse=True)
        return relevant[:limit]


# Global matcher instance
_matcher = None

def get_matcher() -> SemanticMatcher:
    """Get or create semantic matcher instance"""
    global _matcher
    if _matcher is None:
        _matcher = SemanticMatcher()
    return _matcher
