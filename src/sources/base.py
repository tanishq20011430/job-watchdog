"""
Base class for async job sources.
All sources inherit from this and implement fetch_jobs().
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime
import httpx
import logging

from ..database.models import RawJob, JobBatch
from ..config.settings import settings

logger = logging.getLogger(__name__)


class BaseJobSource(ABC):
    """Abstract base class for all job sources"""
    
    name: str = "Unknown"
    base_url: str = ""
    rate_limit_seconds: float = 1.0
    timeout: int = 30
    
    def __init__(self):
        self.client: Optional[httpx.AsyncClient] = None
        self.last_request_time: float = 0
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client"""
        if self.client is None or self.client.is_closed:
            self.client = httpx.AsyncClient(
                timeout=self.timeout,
                headers=self._default_headers(),
                follow_redirects=True
            )
        return self.client
    
    def _default_headers(self) -> dict:
        """Default headers for requests"""
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "en-US,en;q=0.9",
        }
    
    async def _rate_limit(self):
        """Enforce rate limiting between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_seconds:
            await asyncio.sleep(self.rate_limit_seconds - elapsed)
        self.last_request_time = time.time()
    
    async def _request(self, url: str, params: dict = None, method: str = "GET") -> Optional[httpx.Response]:
        """Make HTTP request with rate limiting and error handling"""
        await self._rate_limit()
        client = await self._get_client()
        
        try:
            if method == "GET":
                response = await client.get(url, params=params)
            else:
                response = await client.post(url, data=params)
            
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            logger.warning(f"{self.name}: HTTP {e.response.status_code} from {url}")
            return None
        except httpx.TimeoutException:
            logger.warning(f"{self.name}: Timeout from {url}")
            return None
        except Exception as e:
            logger.error(f"{self.name}: Error - {str(e)[:100]}")
            return None
    
    async def close(self):
        """Close HTTP client"""
        if self.client and not self.client.is_closed:
            await self.client.aclose()
    
    @abstractmethod
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        """
        Fetch jobs matching keywords.
        Must be implemented by each source.
        Returns a JobBatch with jobs and metadata.
        """
        pass
    
    def _matches_keywords(self, text: str, keywords: List[str]) -> bool:
        """Check if text contains any of the keywords"""
        text_lower = text.lower()
        return any(kw.lower() in text_lower for kw in keywords)
    
    def _parse_job_age_hours(self, posted: str) -> float:
        """
        Parse posted time and return age in hours.
        Returns float('inf') if unable to parse.
        """
        if not posted:
            return float('inf')
        
        now = datetime.now()
        posted_lower = str(posted).lower().strip()
        
        # Handle common patterns
        if any(x in posted_lower for x in ['just', 'now', 'moment', 'second']):
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
            return 30
        
        if 'day' in posted_lower:
            try:
                days = int(''.join(filter(str.isdigit, posted_lower.split('day')[0])) or '0')
                return days * 24
            except:
                return float('inf')
        
        if 'week' in posted_lower or 'month' in posted_lower:
            return float('inf')
        
        # Try ISO date parsing
        if 'T' in posted or '-' in posted:
            try:
                # Handle various ISO formats
                date_part = posted.split('T')[0] if 'T' in posted else posted[:10]
                if '+' in date_part:
                    date_part = date_part.split('+')[0]
                posted_dt = datetime.strptime(date_part, '%Y-%m-%d')
                age_hours = (now - posted_dt).total_seconds() / 3600
                return max(0, age_hours)
            except:
                pass
        
        # Default to reasonably fresh
        return 12


class RemoteOKSource(BaseJobSource):
    """RemoteOK.com - Free JSON API for remote tech jobs"""
    name = "RemoteOK"
    base_url = "https://remoteok.com/api"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        try:
            response = await self._request(self.base_url)
            if not response:
                return JobBatch(source=self.name, error="Request failed")
            
            data = response.json()
            job_list = data[1:] if len(data) > 1 else []  # First item is metadata
            
            for job in job_list:
                job_text = f"{job.get('position', '')} {job.get('description', '')} {' '.join(job.get('tags', []))}".lower()
                
                if self._matches_keywords(job_text, keywords):
                    jobs.append(RawJob(
                        title=job.get('position', 'Unknown'),
                        company=job.get('company', 'Unknown'),
                        location=job.get('location', 'Remote'),
                        description=job.get('description', ''),
                        url=job.get('url', f"https://remoteok.com/jobs/{job.get('id', '')}"),
                        source=self.name,
                        posted=job.get('date', ''),
                        salary=job.get('salary', ''),
                        raw_data=job
                    ))
            
            logger.info(f"{self.name}: Found {len(jobs)} matches from {len(job_list)} jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class ArbeitnowSource(BaseJobSource):
    """Arbeitnow.com - Free JSON API"""
    name = "Arbeitnow"
    base_url = "https://www.arbeitnow.com/api/job-board-api"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        try:
            response = await self._request(self.base_url)
            if not response:
                return JobBatch(source=self.name, error="Request failed")
            
            data = response.json()
            job_list = data.get('data', [])
            
            for job in job_list:
                job_text = f"{job.get('title', '')} {job.get('description', '')} {' '.join(job.get('tags', []))}".lower()
                
                if self._matches_keywords(job_text, keywords):
                    jobs.append(RawJob(
                        title=job.get('title', 'Unknown'),
                        company=job.get('company_name', 'Unknown'),
                        location=job.get('location', 'Unknown'),
                        description=job.get('description', ''),
                        url=job.get('url', ''),
                        source=self.name,
                        posted=job.get('created_at', ''),
                        raw_data=job
                    ))
            
            logger.info(f"{self.name}: Found {len(jobs)} matches from {len(job_list)} jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class HimalayasSource(BaseJobSource):
    """Himalayas.app - Free API for remote jobs"""
    name = "Himalayas"
    base_url = "https://himalayas.app/jobs/api"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        try:
            response = await self._request(self.base_url, params={"limit": 100})
            if not response:
                return JobBatch(source=self.name, error="Request failed")
            
            data = response.json()
            job_list = data.get('jobs', [])
            
            for job in job_list:
                categories = job.get('categories', [])
                if isinstance(categories, list):
                    categories = ' '.join(categories)
                
                job_text = f"{job.get('title', '')} {job.get('description', '')} {categories}".lower()
                
                if self._matches_keywords(job_text, keywords):
                    location_restrictions = job.get('locationRestrictions', [])
                    location = ', '.join(location_restrictions) if location_restrictions else 'Remote'
                    
                    jobs.append(RawJob(
                        title=job.get('title', 'Unknown'),
                        company=job.get('companyName', 'Unknown'),
                        location=location,
                        description=job.get('description', ''),
                        url=job.get('applicationLink', '') or f"https://himalayas.app/jobs/{job.get('slug', '')}",
                        source=self.name,
                        posted=job.get('pubDate', ''),
                        salary=job.get('salary', ''),
                        raw_data=job
                    ))
            
            logger.info(f"{self.name}: Found {len(jobs)} matches from {len(job_list)} jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class JobicySource(BaseJobSource):
    """Jobicy.com - Free API for remote jobs"""
    name = "Jobicy"
    base_url = "https://jobicy.com/api/v2/remote-jobs"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        try:
            # Fetch data science and general jobs
            industries = ["data-science", "all"]
            seen_ids = set()
            
            for industry in industries:
                params = {"count": 50}
                if industry != "all":
                    params["industry"] = industry
                
                response = await self._request(self.base_url, params=params)
                if not response:
                    continue
                
                data = response.json()
                job_list = data.get('jobs', [])
                
                for job in job_list:
                    job_id = job.get('id', '')
                    if job_id in seen_ids:
                        continue
                    
                    job_text = f"{job.get('jobTitle', '')} {job.get('jobExcerpt', '')} {job.get('jobIndustry', '')}".lower()
                    
                    if self._matches_keywords(job_text, keywords):
                        seen_ids.add(job_id)
                        jobs.append(RawJob(
                            title=job.get('jobTitle', 'Unknown'),
                            company=job.get('companyName', 'Unknown'),
                            location=job.get('jobGeo', 'Remote'),
                            description=job.get('jobExcerpt', ''),
                            url=job.get('url', ''),
                            source=self.name,
                            posted=job.get('pubDate', ''),
                            salary=job.get('annualSalaryMin', ''),
                            job_type=job.get('jobType', ''),
                            raw_data=job
                        ))
            
            logger.info(f"{self.name}: Found {len(jobs)} matches")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class FindworkSource(BaseJobSource):
    """Findwork.dev - Free API for dev jobs"""
    name = "Findwork"
    base_url = "https://findwork.dev/api/jobs/"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        seen_ids = set()
        
        try:
            # Search for each keyword
            for keyword in keywords[:3]:  # Limit to avoid rate limiting
                response = await self._request(self.base_url, params={"search": keyword})
                if not response:
                    continue
                
                data = response.json()
                for job in data.get('results', []):
                    job_id = job.get('id', '')
                    if job_id in seen_ids:
                        continue
                    
                    seen_ids.add(job_id)
                    jobs.append(RawJob(
                        title=job.get('role', 'Unknown'),
                        company=job.get('company_name', 'Unknown'),
                        location=job.get('location', 'Unknown'),
                        description=job.get('text', ''),
                        url=job.get('url', ''),
                        source=self.name,
                        posted=job.get('date_posted', ''),
                        raw_data=job
                    ))
            
            logger.info(f"{self.name}: Found {len(jobs)} matches")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class TheMuseSource(BaseJobSource):
    """TheMuse.com - Free API"""
    name = "TheMuse"
    base_url = "https://www.themuse.com/api/public/jobs"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        seen_ids = set()
        
        try:
            categories = ["Data%20Science", "Data%20and%20Analytics", "Software%20Engineering"]
            
            for category in categories:
                response = await self._request(f"{self.base_url}?category={category}&page=1")
                if not response:
                    continue
                
                data = response.json()
                for job in data.get('results', []):
                    job_id = job.get('id', '')
                    if job_id in seen_ids:
                        continue
                    
                    job_text = f"{job.get('name', '')} {job.get('contents', '')}".lower()
                    
                    if self._matches_keywords(job_text, keywords):
                        seen_ids.add(job_id)
                        company = job.get('company', {})
                        locations = job.get('locations', [{}])
                        
                        jobs.append(RawJob(
                            title=job.get('name', 'Unknown'),
                            company=company.get('name', 'Unknown') if isinstance(company, dict) else 'Unknown',
                            location=locations[0].get('name', 'Unknown') if locations else 'Unknown',
                            description=job.get('contents', ''),
                            url=job.get('refs', {}).get('landing_page', ''),
                            source=self.name,
                            posted=job.get('publication_date', ''),
                            raw_data=job
                        ))
            
            logger.info(f"{self.name}: Found {len(jobs)} matches")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class HNHiringSource(BaseJobSource):
    """Hacker News Who is Hiring threads"""
    name = "HN-Hiring"
    base_url = "https://hn.algolia.com/api/v1"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        try:
            import re
            from datetime import datetime, timedelta
            
            # Get current month/year for filtering
            now = datetime.now()
            current_month = now.strftime("%B")  # e.g., "February"
            current_year = str(now.year)  # e.g., "2026"
            last_month = (now - timedelta(days=30)).strftime("%B")
            
            # Find latest hiring thread - search by "Ask HN: Who is hiring"
            # Use numericFilters to only get recent posts (last 60 days)
            sixty_days_ago = int((now - timedelta(days=60)).timestamp())
            
            response = await self._request(
                f"{self.base_url}/search",
                params={
                    "query": "Ask HN Who is hiring",
                    "tags": "story,ask_hn",
                    "hitsPerPage": 10,
                    "numericFilters": f"created_at_i>{sixty_days_ago}"
                }
            )
            if not response:
                return JobBatch(source=self.name, error="Failed to find hiring thread")
            
            data = response.json()
            thread_id = None
            thread_title = None
            
            for hit in data.get('hits', []):
                title = hit.get('title', '')
                # Match "Ask HN: Who is hiring? (Month Year)" pattern
                if ('Who is hiring' in title or "Who's hiring" in title) and \
                   (current_month in title or last_month in title or current_year in title):
                    thread_id = hit.get('objectID')
                    thread_title = title
                    break
            
            # Fallback: take the most recent hiring thread
            if not thread_id:
                for hit in data.get('hits', []):
                    title = hit.get('title', '')
                    if 'Who is hiring' in title or "Who's hiring" in title:
                        thread_id = hit.get('objectID')
                        thread_title = title
                        break
            
            if not thread_id:
                return JobBatch(source=self.name, error="No recent hiring thread found")
            
            logger.info(f"{self.name}: Using thread '{thread_title}' (ID: {thread_id})")
            
            # Get comments from this thread
            response = await self._request(
                f"{self.base_url}/search",
                params={"tags": f"comment,story_{thread_id}", "hitsPerPage": 100}
            )
            if not response:
                return JobBatch(source=self.name, error="Failed to fetch comments")
            
            comments = response.json().get('hits', [])
            keywords_lower = [k.lower() for k in keywords]
            
            for comment in comments:
                text = comment.get('comment_text', '')
                if not text:
                    continue
                
                # Clean HTML
                text_clean = re.sub(r'<[^>]+>', ' ', text)
                text_lower = text_clean.lower()
                
                if any(kw in text_lower for kw in keywords_lower):
                    lines = text_clean.strip().split('\n')
                    first_line = lines[0] if lines else ''
                    
                    # Extract company
                    company = 'HN Company'
                    if '|' in first_line:
                        company = first_line.split('|')[0].strip()[:50]
                    elif len(first_line) < 60:
                        company = first_line[:50]
                    
                    # Extract title hint
                    title = 'Tech Position'
                    if 'data scientist' in text_lower:
                        title = 'Data Scientist'
                    elif 'data analyst' in text_lower:
                        title = 'Data Analyst'
                    elif 'machine learning' in text_lower or 'ml' in text_lower:
                        title = 'ML Engineer'
                    elif 'engineer' in text_lower:
                        title = 'Software Engineer'
                    
                    jobs.append(RawJob(
                        title=title,
                        company=company,
                        location='Remote/Various',
                        description=text_clean[:2000],
                        url=f"https://news.ycombinator.com/item?id={comment.get('objectID', '')}",
                        source=self.name,
                        posted=comment.get('created_at', '')
                    ))
                    
                    if len(jobs) >= 25:
                        break
            
            logger.info(f"{self.name}: Found {len(jobs)} matches")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )
