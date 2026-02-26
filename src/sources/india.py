"""
India-focused job sources.
Optimized for Pune, Mumbai, Bangalore, Hyderabad, Delhi NCR regions.
"""

import asyncio
import time
import re
from typing import List, Optional
from datetime import datetime
import logging

from ..database.models import RawJob, JobBatch
from ..config.settings import settings
from .base import BaseJobSource

logger = logging.getLogger(__name__)


class NaukriSource(BaseJobSource):
    """
    Naukri.com scraper using direct API endpoints.
    Naukri is the largest job portal in India.
    """
    name = "Naukri"
    base_url = "https://www.naukri.com/jobapi/v3/search"
    rate_limit_seconds = 2.0
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        seen_ids = set()
        
        try:
            # Custom headers to mimic browser
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://www.naukri.com/",
                "appid": "109",
                "systemid": "109",
            }
            
            # Build client with custom headers
            import httpx
            async with httpx.AsyncClient(timeout=30, headers=headers) as client:
                for keyword in keywords[:3]:
                    try:
                        # Naukri API parameters
                        params = {
                            "noOfResults": 50,
                            "urlType": "search_by_keyword",
                            "searchType": "adv",
                            "keyword": keyword,
                            "pageNo": 1,
                            "sort": "f",  # Sort by freshness
                            "seoKey": keyword.replace(" ", "-"),
                            "src": "jobsearchDesk",
                            "latLong": "",
                        }
                        
                        response = await client.get(self.base_url, params=params)
                        
                        if response.status_code != 200:
                            logger.warning(f"{self.name}: HTTP {response.status_code}")
                            continue
                        
                        data = response.json()
                        job_list = data.get('jobDetails', [])
                        
                        for job in job_list:
                            job_id = job.get('jobId', '')
                            if job_id in seen_ids:
                                continue
                            
                            seen_ids.add(job_id)
                            
                            # Extract location
                            placeholders = job.get('placeholders', [])
                            location = 'India'
                            for ph in placeholders:
                                if ph.get('type') == 'location':
                                    location = ph.get('label', 'India')
                                    break
                            
                            # Extract experience
                            experience = ''
                            for ph in placeholders:
                                if ph.get('type') == 'experience':
                                    experience = ph.get('label', '')
                                    break
                            
                            jobs.append(RawJob(
                                title=job.get('title', 'Unknown'),
                                company=job.get('companyName', 'Unknown'),
                                location=location,
                                description=job.get('jobDescription', ''),
                                url=f"https://www.naukri.com{job.get('jdURL', '')}",
                                source=self.name,
                                posted=job.get('footerPlaceholderLabel', ''),
                                salary=job.get('placeholders', [{}])[0].get('label', '') if job.get('placeholders') else '',
                                job_type=experience,
                                raw_data=job
                            ))
                        
                        await asyncio.sleep(1.5)  # Rate limiting
                        
                    except Exception as e:
                        logger.warning(f"{self.name}: Error for {keyword}: {str(e)[:100]}")
                        continue
            
            logger.info(f"{self.name}: Found {len(jobs)} jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class FounditSource(BaseJobSource):
    """
    Foundit.in (formerly Monster India) job source.
    """
    name = "Foundit"
    base_url = "https://www.foundit.in/middleware/jobsearch"
    rate_limit_seconds = 2.0
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        seen_ids = set()
        
        try:
            import httpx
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://www.foundit.in/",
            }
            
            async with httpx.AsyncClient(timeout=30, headers=headers) as client:
                for keyword in keywords[:2]:
                    try:
                        params = {
                            "query": keyword,
                            "limit": 30,
                            "sort": "1",  # Sort by date
                        }
                        
                        response = await client.get(self.base_url, params=params)
                        
                        if response.status_code != 200:
                            continue
                        
                        data = response.json()
                        job_list = data.get('jobSearchResponse', {}).get('data', [])
                        
                        for job in job_list:
                            job_id = job.get('jobId', '')
                            if job_id in seen_ids:
                                continue
                            
                            seen_ids.add(job_id)
                            jobs.append(RawJob(
                                title=job.get('title', 'Unknown'),
                                company=job.get('companyName', 'Unknown'),
                                location=job.get('locations', ['India'])[0] if job.get('locations') else 'India',
                                description=job.get('jobDescription', ''),
                                url=job.get('jobDetailUrl', ''),
                                source=self.name,
                                posted=job.get('postedDate', ''),
                                salary=job.get('salary', ''),
                                raw_data=job
                            ))
                        
                        await asyncio.sleep(1.5)
                        
                    except Exception as e:
                        logger.warning(f"{self.name}: Error for {keyword}: {str(e)[:100]}")
                        continue
            
            logger.info(f"{self.name}: Found {len(jobs)} jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class InstahyreSource(BaseJobSource):
    """
    Instahyre - Curated tech jobs in India.
    Popular for startups and tech companies.
    """
    name = "Instahyre"
    base_url = "https://www.instahyre.com/api/v1/search_jobs/"
    rate_limit_seconds = 2.0
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        try:
            import httpx
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://www.instahyre.com/",
            }
            
            async with httpx.AsyncClient(timeout=30, headers=headers) as client:
                # Instahyre uses POST with filters
                for keyword in keywords[:2]:
                    try:
                        payload = {
                            "job_type": "",
                            "min_experience": 0,
                            "max_experience": 5,
                            "location": ["pune", "mumbai", "bangalore", "hyderabad", "delhi"],
                            "skills": [keyword],
                            "page": 1,
                        }
                        
                        response = await client.post(self.base_url, json=payload)
                        
                        if response.status_code != 200:
                            continue
                        
                        data = response.json()
                        job_list = data.get('jobs', [])
                        
                        for job in job_list:
                            jobs.append(RawJob(
                                title=job.get('title', 'Unknown'),
                                company=job.get('company', {}).get('name', 'Unknown'),
                                location=', '.join(job.get('locations', ['India'])),
                                description=job.get('description', ''),
                                url=f"https://www.instahyre.com/job/{job.get('slug', '')}",
                                source=self.name,
                                posted=job.get('created_at', ''),
                                salary=job.get('salary_range', ''),
                                raw_data=job
                            ))
                        
                        await asyncio.sleep(1.5)
                        
                    except Exception as e:
                        logger.warning(f"{self.name}: Error for {keyword}: {str(e)[:100]}")
                        continue
            
            logger.info(f"{self.name}: Found {len(jobs)} jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class CutshortSource(BaseJobSource):
    """
    Cutshort - AI-powered job matching for tech professionals.
    Strong in the Indian startup ecosystem.
    """
    name = "Cutshort"
    base_url = "https://cutshort.io/api/public/jobs"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        try:
            response = await self._request(
                self.base_url,
                params={"limit": 50, "skills": ",".join(keywords[:3])}
            )
            
            if not response:
                return JobBatch(source=self.name, error="Request failed")
            
            data = response.json()
            job_list = data.get('jobs', data) if isinstance(data, dict) else data
            
            if isinstance(job_list, list):
                for job in job_list[:50]:
                    jobs.append(RawJob(
                        title=job.get('title', 'Unknown'),
                        company=job.get('company', {}).get('name', 'Unknown') if isinstance(job.get('company'), dict) else 'Unknown',
                        location=job.get('location', 'India'),
                        description=job.get('description', ''),
                        url=job.get('url', f"https://cutshort.io/job/{job.get('id', '')}"),
                        source=self.name,
                        posted=job.get('posted_at', ''),
                        salary=job.get('salary', ''),
                        raw_data=job
                    ))
            
            logger.info(f"{self.name}: Found {len(jobs)} jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class HiristSource(BaseJobSource):
    """
    Hirist - IT jobs in India.
    """
    name = "Hirist"
    base_url = "https://www.hirist.tech/api/jobs/search"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        try:
            import httpx
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            }
            
            async with httpx.AsyncClient(timeout=30, headers=headers) as client:
                for keyword in keywords[:2]:
                    try:
                        params = {"q": keyword, "page": 1, "limit": 30}
                        response = await client.get(self.base_url, params=params)
                        
                        if response.status_code != 200:
                            continue
                        
                        data = response.json()
                        job_list = data.get('data', data.get('jobs', []))
                        
                        for job in job_list:
                            jobs.append(RawJob(
                                title=job.get('title', job.get('designation', 'Unknown')),
                                company=job.get('company', job.get('company_name', 'Unknown')),
                                location=job.get('location', 'India'),
                                description=job.get('description', job.get('job_description', '')),
                                url=job.get('url', job.get('apply_url', '')),
                                source=self.name,
                                posted=job.get('posted_date', ''),
                                raw_data=job
                            ))
                        
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.warning(f"{self.name}: Error: {str(e)[:100]}")
                        continue
            
            logger.info(f"{self.name}: Found {len(jobs)} jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class LinkedInIndiaSource(BaseJobSource):
    """
    LinkedIn Jobs via public API (no auth required).
    Limited but useful for discovery.
    """
    name = "LinkedIn"
    base_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    rate_limit_seconds = 3.0
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        try:
            import httpx
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
            }
            
            async with httpx.AsyncClient(timeout=30, headers=headers, follow_redirects=True) as client:
                for keyword in keywords[:2]:
                    try:
                        params = {
                            "keywords": keyword,
                            "location": "India",
                            "f_TPR": "r86400",  # Last 24 hours
                            "start": 0,
                        }
                        
                        response = await client.get(self.base_url, params=params)
                        
                        if response.status_code != 200:
                            continue
                        
                        html = response.text
                        
                        # Parse job cards from HTML
                        card_pattern = r'<li[^>]*>.*?<a[^>]*href="([^"]*linkedin\.com/jobs/view/[^"]*)"[^>]*>.*?<span[^>]*>([^<]+)</span>.*?<h4[^>]*>([^<]+)</h4>.*?<span[^>]*class="job-search-card__location"[^>]*>([^<]+)</span>.*?</li>'
                        
                        # Simpler extraction
                        job_urls = re.findall(r'href="(https://www\.linkedin\.com/jobs/view/[^"]+)"', html)
                        job_titles = re.findall(r'<span class="sr-only">([^<]+)</span>', html)
                        
                        for i, url in enumerate(job_urls[:20]):
                            title = job_titles[i] if i < len(job_titles) else f"{keyword} position"
                            
                            jobs.append(RawJob(
                                title=title,
                                company='LinkedIn Listing',
                                location='India',
                                description=title,
                                url=url.split('?')[0],
                                source=self.name,
                                posted='Recent',
                                raw_data={"url": url}
                            ))
                        
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        logger.warning(f"{self.name}: Error: {str(e)[:100]}")
                        continue
            
            logger.info(f"{self.name}: Found {len(jobs)} jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class GoogleJobsSource(BaseJobSource):
    """
    Google Jobs via SerpAPI.
    Best source for fresh, relevant India jobs.
    Uses smart keyword rotation to maximize coverage with limited API calls.
    """
    name = "Google-Jobs"
    rate_limit_seconds = 1.0
    
    # Rotating keyword groups for comprehensive coverage
    KEYWORD_GROUPS = [
        ["data scientist", "machine learning engineer"],
        ["data analyst", "business analyst"],
        ["power bi developer", "tableau analyst"],
        ["python developer data", "sql analyst"],
        ["ai engineer", "nlp engineer"],
        ["data engineer", "analytics engineer"],
    ]
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        from ..database.repository import db
        
        start = time.time()
        jobs = []
        error = None
        
        # Check quota
        usage = db.get_api_usage("serpapi")
        remaining = settings.serpapi.monthly_limit - usage
        
        if remaining <= 0:
            logger.warning(f"{self.name}: Monthly limit reached ({usage}/{settings.serpapi.monthly_limit})")
            return JobBatch(source=self.name, error="Monthly limit reached")
        
        if not settings.serpapi.api_key:
            return JobBatch(source=self.name, error="SERPAPI_KEY not configured")
        
        try:
            import httpx
            from datetime import datetime
            
            # Smart rotation: use day of month to rotate through keyword groups
            day = datetime.now().day
            group_index = day % len(self.KEYWORD_GROUPS)
            search_keywords = self.KEYWORD_GROUPS[group_index]
            
            # Also add first keyword from user's list if different
            if keywords and keywords[0].lower() not in [k.lower() for k in search_keywords]:
                search_keywords = [keywords[0]] + search_keywords[:1]
            
            # Limit API calls based on remaining quota
            max_searches = min(3, remaining)  # Max 3 searches per run
            search_keywords = search_keywords[:max_searches]
            
            logger.info(f"{self.name}: Searching {len(search_keywords)} keywords: {search_keywords}")
            
            async with httpx.AsyncClient(timeout=30) as client:
                for keyword in search_keywords:
                    params = {
                        "engine": "google_jobs",
                        "q": f"{keyword} jobs",
                        "location": "India",
                        "hl": "en",
                        "gl": "in",
                        "chips": "date_posted:today",  # Fresh jobs only (last 24h)
                        "api_key": settings.serpapi.api_key
                    }
                    
                    response = await client.get("https://serpapi.com/search", params=params)
                    
                    if response.status_code != 200:
                        logger.warning(f"{self.name}: HTTP {response.status_code} for '{keyword}'")
                        continue
                    
                    # Track usage
                    db.increment_api_usage("serpapi")
                    
                    data = response.json()
                    job_results = data.get('jobs_results', [])
                    
                    for job in job_results[:15]:  # Top 15 per keyword
                        # Get apply link
                        apply_options = job.get('apply_options', [])
                        url = apply_options[0].get('link', '') if apply_options else job.get('share_link', '')
                        
                        # Get job metadata
                        detected_extensions = job.get('detected_extensions', {})
                        posted = detected_extensions.get('posted_at', '')
                        schedule = detected_extensions.get('schedule_type', '')
                        salary = detected_extensions.get('salary', '')
                        
                        jobs.append(RawJob(
                            title=job.get('title', 'Unknown'),
                            company=job.get('company_name', 'Unknown'),
                            location=job.get('location', 'India'),
                            description=job.get('description', '')[:2000],
                            url=url,
                            source=self.name,
                            posted=posted,
                            salary=salary,
                            job_type=schedule,
                            raw_data=job
                        ))
                    
                    # Rate limit between searches
                    await asyncio.sleep(0.5)
            
            # Remove duplicates by title+company
            seen = set()
            unique_jobs = []
            for job in jobs:
                key = f"{job.title.lower()}|{job.company.lower()}"
                if key not in seen:
                    seen.add(key)
                    unique_jobs.append(job)
            jobs = unique_jobs
            
            new_usage = db.get_api_usage("serpapi")
            logger.info(f"{self.name}: Found {len(jobs)} unique jobs (API: {new_usage}/{settings.serpapi.monthly_limit})")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class GoogleJobsDirectSource(BaseJobSource):
    """
    DISABLED: Google Jobs scraper triggers CAPTCHA.
    Use GoogleJobsSource (SerpAPI) or Adzuna instead.
    """
    name = "Google-Jobs-Free"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        # Google blocks direct scraping with CAPTCHA
        return JobBatch(
            source=self.name, 
            error="Disabled: Google blocks direct scraping. Use SerpAPI or Adzuna."
        )


class GreenhouseMultiSource(BaseJobSource):
    """
    Greenhouse API - FREE, NO KEY NEEDED!
    Fetches jobs from multiple top companies that use Greenhouse.
    Each company has a public job board API.
    """
    name = "Greenhouse"
    rate_limit_seconds = 0.5
    
    # Top companies using Greenhouse with India/Remote jobs
    COMPANIES = [
        "airbnb", "pinterest", "cloudflare", "stripe", "figma",
        "discord", "instacart", "coinbase", "databricks", "notion",
        "gitlab", "elastic", "hashicorp", "datadog", "confluent",
        "mongodb", "snowflake", "plaid", "brex", "ramp",
    ]
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=15) as client:
                # Rotate through 5 companies per run to stay fast
                hour = datetime.now().hour
                start_idx = (hour % 4) * 5
                companies_to_check = self.COMPANIES[start_idx:start_idx+5]
                
                logger.info(f"{self.name}: Checking {companies_to_check}")
                
                for company in companies_to_check:
                    try:
                        url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
                        response = await client.get(url)
                        
                        if response.status_code != 200:
                            continue
                        
                        data = response.json()
                        company_jobs = data.get('jobs', [])
                        
                        for job in company_jobs:
                            title = job.get('title', '').lower()
                            location = job.get('location', {}).get('name', '')
                            
                            # Filter for relevant jobs (data/analytics/ML)
                            is_relevant = any(kw in title for kw in keywords_lower)
                            
                            # Filter for India or Remote
                            location_lower = location.lower()
                            is_india_remote = any(loc in location_lower for loc in [
                                'india', 'bangalore', 'bengaluru', 'mumbai', 'pune', 
                                'hyderabad', 'delhi', 'gurgaon', 'noida', 'chennai',
                                'remote', 'anywhere', 'worldwide', 'global'
                            ])
                            
                            if is_relevant and is_india_remote:
                                jobs.append(RawJob(
                                    title=job.get('title', 'Unknown'),
                                    company=company.title(),
                                    location=location,
                                    description="",  # Need separate API call for description
                                    url=job.get('absolute_url', ''),
                                    source=self.name,
                                    posted=job.get('updated_at', ''),
                                    job_type="",
                                ))
                        
                        await asyncio.sleep(0.3)
                        
                    except Exception as e:
                        logger.debug(f"{self.name}: Error fetching {company}: {e}")
                        continue
            
            logger.info(f"{self.name}: Found {len(jobs)} jobs (FREE - no API key)")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class WorkingNomadsSource(BaseJobSource):
    """
    WorkingNomads API - FREE, NO KEY NEEDED!
    Remote jobs with good data/tech coverage.
    """
    name = "WorkingNomads"
    base_url = "https://www.workingnomads.com/api/exposed_jobs/"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(self.base_url)
                
                if response.status_code != 200:
                    return JobBatch(source=self.name, error=f"HTTP {response.status_code}")
                
                data = response.json()
                
                for job in data:
                    title = job.get('title', '').lower()
                    category = job.get('category_name', '').lower()
                    
                    # Filter for relevant jobs
                    is_relevant = any(kw in title or kw in category for kw in keywords_lower)
                    
                    if is_relevant:
                        jobs.append(RawJob(
                            title=job.get('title', 'Unknown'),
                            company=job.get('company_name', 'Unknown'),
                            location=job.get('location', 'Remote'),
                            description=job.get('description', '')[:2000],
                            url=job.get('url', ''),
                            source=self.name,
                            posted=job.get('pub_date', ''),
                            job_type=job.get('category_name', ''),
                        ))
            
            logger.info(f"{self.name}: Found {len(jobs)} remote jobs (FREE - no API key)")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class AdzunaIndiaSource(BaseJobSource):
    """
    Adzuna API - FREE tier with 1000 calls/month!
    Great for India jobs with fresh listings.
    Sign up at: https://developer.adzuna.com/
    """
    name = "Adzuna-India"
    base_url = "https://api.adzuna.com/v1/api/jobs/in/search/1"
    rate_limit_seconds = 1.0
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        # Check for API credentials
        app_id = settings.adzuna_app_id if hasattr(settings, 'adzuna_app_id') else None
        app_key = settings.adzuna_app_key if hasattr(settings, 'adzuna_app_key') else None
        
        if not app_id or not app_key:
            # Try environment variables
            import os
            app_id = os.getenv('ADZUNA_APP_ID')
            app_key = os.getenv('ADZUNA_APP_KEY')
        
        if not app_id or not app_key:
            return JobBatch(
                source=self.name, 
                error="ADZUNA_APP_ID and ADZUNA_APP_KEY not configured. Sign up free at https://developer.adzuna.com/"
            )
        
        try:
            import httpx
            
            # Search with multiple keywords
            search_terms = keywords[:3] if keywords else ["data analyst", "data scientist"]
            
            async with httpx.AsyncClient(timeout=30) as client:
                for term in search_terms:
                    params = {
                        "app_id": app_id,
                        "app_key": app_key,
                        "results_per_page": 20,
                        "what": term,
                        "where": "India",
                        "max_days_old": 1,  # Only last 24 hours
                        "sort_by": "date",
                        "content-type": "application/json",
                    }
                    
                    response = await client.get(self.base_url, params=params)
                    
                    if response.status_code != 200:
                        logger.warning(f"{self.name}: HTTP {response.status_code} for '{term}'")
                        continue
                    
                    data = response.json()
                    results = data.get('results', [])
                    
                    for job in results:
                        jobs.append(RawJob(
                            title=job.get('title', 'Unknown'),
                            company=job.get('company', {}).get('display_name', 'Unknown'),
                            location=job.get('location', {}).get('display_name', 'India'),
                            description=job.get('description', '')[:2000],
                            url=job.get('redirect_url', ''),
                            source=self.name,
                            posted=job.get('created', ''),
                            salary=job.get('salary_min', ''),
                            job_type=job.get('contract_type', ''),
                        ))
                    
                    await asyncio.sleep(0.3)
            
            # Deduplicate
            seen = set()
            unique_jobs = []
            for job in jobs:
                key = f"{job.title.lower()}|{job.company.lower()}"
                if key not in seen:
                    seen.add(key)
                    unique_jobs.append(job)
            jobs = unique_jobs
            
            logger.info(f"{self.name}: Found {len(jobs)} unique jobs (FREE API)")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class IndeedIndiaPlaywrightSource(BaseJobSource):
    """
    Indeed India scraper using Playwright for stealth.
    Requires playwright and playwright-stealth to be installed.
    """
    name = "Indeed-India"
    rate_limit_seconds = 3.0
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        try:
            from playwright.async_api import async_playwright
            
            try:
                from playwright_stealth import Stealth
                has_stealth = True
            except ImportError:
                has_stealth = False
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = await context.new_page()
                
                if has_stealth:
                    stealth = Stealth()
                    await stealth.apply_stealth_async(page)
                
                seen_ids = set()
                
                for keyword in keywords[:2]:
                    try:
                        # fromage=1 = last 24 hours, sort=date
                        search_url = f"https://in.indeed.com/jobs?q={keyword}&l=India&sort=date&fromage=1"
                        await page.goto(search_url, timeout=30000)
                        await page.wait_for_timeout(2000)
                        
                        job_cards = await page.query_selector_all(".job_seen_beacon, [data-jk]")
                        
                        for card in job_cards[:20]:
                            try:
                                title_el = await card.query_selector("h2.jobTitle a, .jobTitle a")
                                company_el = await card.query_selector("[data-testid='company-name'], .companyName")
                                location_el = await card.query_selector("[data-testid='text-location'], .companyLocation")
                                date_el = await card.query_selector(".date, .result-footer")
                                
                                title = await title_el.inner_text() if title_el else "Unknown"
                                company = await company_el.inner_text() if company_el else "Unknown"
                                location = await location_el.inner_text() if location_el else "India"
                                posted = await date_el.inner_text() if date_el else "Today"
                                
                                href = await title_el.get_attribute("href") if title_el else ""
                                url = f"https://in.indeed.com{href}" if href and not href.startswith("http") else href
                                
                                job_key = f"{title}_{company}"
                                if job_key not in seen_ids and title != "Unknown":
                                    seen_ids.add(job_key)
                                    jobs.append(RawJob(
                                        title=title[:100],
                                        company=company[:50] if company else 'Unknown',
                                        location=location,
                                        description=f"{title} at {company}",
                                        url=url,
                                        source=self.name,
                                        posted=posted
                                    ))
                            except Exception as e:
                                continue
                        
                        await page.wait_for_timeout(2000)
                        
                    except Exception as e:
                        logger.warning(f"{self.name}: Error for {keyword}: {str(e)[:100]}")
                        continue
                
                await browser.close()
            
            logger.info(f"{self.name}: Found {len(jobs)} jobs")
            
        except ImportError:
            error = "Playwright not installed. Run: pip install playwright playwright-stealth && playwright install chromium"
            logger.warning(f"{self.name}: {error}")
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


# Company career page sources for Pune tech hubs
class PuneTechCompaniesSource(BaseJobSource):
    """
    Direct career page scraping for major Pune tech companies.
    NVIDIA, Mastercard, Veritas, etc.
    """
    name = "Pune-Tech"
    
    # Career page endpoints
    COMPANY_APIS = [
        {
            "name": "NVIDIA",
            "url": "https://nvidia.wd5.myworkdayjobs.com/wday/cxs/nvidia/NVIDIAExternalCareerSite/jobs",
            "method": "POST",
            "payload": {"searchText": "data", "locationsIdsList": ["5f52c2feb24c01010aaa0605f6250000"]},  # India
        },
        {
            "name": "Mastercard",
            "url": "https://careers.mastercard.com/us/en/search-results?keywords=data&location=Pune",
            "method": "GET",
        },
    ]
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        
        # This is a placeholder - actual implementation would need
        # company-specific API handling
        logger.info(f"{self.name}: Placeholder - implement company-specific scrapers")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )
