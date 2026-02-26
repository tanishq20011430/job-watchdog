"""
FREE job APIs that don't require API keys.
These sources significantly increase job coverage at no cost.
"""

import asyncio
import time
import re
from typing import List, Optional, Set
from datetime import datetime
import logging

from ..database.models import RawJob, JobBatch
from ..config.settings import settings
from .base import BaseJobSource

logger = logging.getLogger(__name__)


class GreenhouseMultiSource(BaseJobSource):
    """
    Greenhouse API - FREE, NO KEY NEEDED!
    Fetches jobs from multiple top tech companies that use Greenhouse.
    API: https://boards-api.greenhouse.io/v1/boards/{company}/jobs
    """
    name = "Greenhouse"
    rate_limit_seconds = 0.3
    
    # 100+ companies using Greenhouse with global/remote jobs
    COMPANIES = [
        # Tech Giants
        "airbnb", "pinterest", "cloudflare", "stripe", "figma",
        "discord", "instacart", "coinbase", "databricks", "notion",
        "gitlab", "elastic", "hashicorp", "datadog", "confluent",
        "mongodb", "snowflake", "plaid", "brex", "ramp",
        # More tech companies
        "twilio", "okta", "zscaler", "pagerduty", "splunk",
        "nutanix", "rubrik", "cohesity", "wework", "gusto", 
        "airtable", "asana", "clickup", "miro", "loom", 
        "calendly", "typeform", "contentful", "sanity",
        # Finance/Fintech
        "robinhood", "square", "affirm", "chime", "sofi",
        "wise", "revolut", "monzo", "klarna", "nubank",
        # AI/ML focused
        "openai", "anthropic", "cohere", "huggingface", "weights-and-biases",
        "scale", "labelbox", "snorkel", "tecton", "anyscale",
        "dbt-labs", "fivetran", "airbyte", "prefect", "dagster",
        # Growth companies
        "flexport", "faire", "rippling", "deel", "remote",
        "lattice", "lever", "greenhouse", "gem", "ashby",
        # India presence
        "razorpay", "meesho", "cred", "zerodha", "groww",
        "swiggy", "zomato", "freshworks", "zoho", "clevertap",
        # More unicorns
        "canva", "atlassian", "doordash", "gopuff", "checkout",
        "bolt", "getir", "gorillas", "flink", "picnic",
        "messagebird", "mollie", "adyen", "mambu", "solarisbank",
        # Developer tools
        "vercel", "supabase", "planetscale", "cockroachlabs", "timescale",
        "neo4j", "redis", "couchbase", "scylladb", "yugabyte",
        "grafana", "newrelic", "sentry", "launchdarkly", "split",
        # Security
        "crowdstrike", "snyk", "lacework", "wiz", "orca-security",
        "cyberark", "1password", "bitwarden", "dashlane", "keeper",
        # E-commerce
        "shopify", "bigcommerce", "commercetools", "salsify", "akeneo",
    ]
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        seen_ids: Set[str] = set()
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=15) as client:
                # Rotate through companies - check 15 companies per run
                hour = datetime.now().hour
                minute = datetime.now().minute // 15  # 0-3
                start_idx = ((hour % 4) * 4 + minute) * 15
                companies_to_check = self.COMPANIES[start_idx:start_idx+15]
                
                # If we've gone past the list, wrap around
                if not companies_to_check:
                    companies_to_check = self.COMPANIES[:15]
                
                logger.info(f"{self.name}: Checking {len(companies_to_check)} companies")
                
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
                            location = job.get('location', {}).get('name', '') if isinstance(job.get('location'), dict) else str(job.get('location', ''))
                            
                            # Filter for relevant jobs (data/analytics/ML/python)
                            is_relevant = any(kw in title for kw in keywords_lower)
                            
                            # Filter for India or Remote
                            location_lower = location.lower()
                            is_india_remote = any(loc in location_lower for loc in [
                                'india', 'bangalore', 'bengaluru', 'mumbai', 'pune', 
                                'hyderabad', 'delhi', 'gurgaon', 'noida', 'chennai',
                                'remote', 'anywhere', 'worldwide', 'global', 'apac'
                            ])
                            
                            job_key = f"{job.get('title', '')}_{company}"
                            
                            if is_relevant and is_india_remote and job_key not in seen_ids:
                                seen_ids.add(job_key)
                                jobs.append(RawJob(
                                    title=job.get('title', 'Unknown'),
                                    company=company.replace('-', ' ').title(),
                                    location=location,
                                    description="",
                                    url=job.get('absolute_url', ''),
                                    source=self.name,
                                    posted=job.get('updated_at', ''),
                                    job_type="",
                                ))
                        
                        await asyncio.sleep(0.2)
                        
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


class LeverMultiSource(BaseJobSource):
    """
    Lever API - FREE, NO KEY NEEDED!
    API: https://api.lever.co/v0/postings/{company}?mode=json
    """
    name = "Lever"
    rate_limit_seconds = 0.3
    
    # Companies using Lever
    COMPANIES = [
        "stripe", "netflix", "spotify", "shopify", "lyft",
        "doordash", "uber", "cruise", "waymo", "aurora",
        "nuro", "zoox", "aptiv", "palantir", "databricks",
        "snowflake", "fivetran", "dbt-labs", "airbyte", "segment",
        "amplitude", "mixpanel", "heap", "fullstory", "pendo",
        "walkme", "whatfix", "appcues", "intercom", "drift",
        "qualified", "gong", "chorus", "outreach", "salesloft",
        "apollo", "zoominfo", "clearbit", "6sense", "demandbase",
        "samsara", "verkada", "dragos", "cloudflare", "fastly",
        # More companies
        "twitch", "snap", "pinterest", "dropbox", "box",
        "zoom", "webex", "ringcentral", "vonage", "bandwidth",
        "plaid", "marqeta", "galileo", "lithic", "unit",
        "mercury", "meow", "brex", "ramp", "divvy",
        # AI/ML
        "replicate", "modal", "banana-dev", "baseten", "beam",
        "lightning-ai", "determined-ai", "grid-ai", "mosaicml", "together",
    ]
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        seen_ids: Set[str] = set()
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=15) as client:
                # Check 12 companies per run
                hour = datetime.now().hour
                start_idx = (hour % 4) * 12
                companies_to_check = self.COMPANIES[start_idx:start_idx+12]
                
                if not companies_to_check:
                    companies_to_check = self.COMPANIES[:12]
                
                logger.info(f"{self.name}: Checking {len(companies_to_check)} companies")
                
                for company in companies_to_check:
                    try:
                        url = f"https://api.lever.co/v0/postings/{company}?mode=json"
                        response = await client.get(url)
                        
                        if response.status_code != 200:
                            continue
                        
                        data = response.json()
                        
                        for job in data:
                            title = job.get('text', '').lower()
                            categories = job.get('categories', {})
                            location = categories.get('location', 'Remote')
                            
                            # Filter for relevant jobs
                            is_relevant = any(kw in title for kw in keywords_lower)
                            
                            # Filter for India or Remote
                            location_lower = location.lower()
                            is_india_remote = any(loc in location_lower for loc in [
                                'india', 'bangalore', 'bengaluru', 'mumbai', 'pune',
                                'hyderabad', 'delhi', 'gurgaon', 'noida', 'chennai',
                                'remote', 'anywhere', 'worldwide', 'global', 'apac'
                            ])
                            
                            job_key = f"{job.get('text', '')}_{company}"
                            
                            if is_relevant and is_india_remote and job_key not in seen_ids:
                                seen_ids.add(job_key)
                                jobs.append(RawJob(
                                    title=job.get('text', 'Unknown'),
                                    company=company.replace('-', ' ').title(),
                                    location=location,
                                    description=job.get('descriptionPlain', '')[:2000],
                                    url=job.get('hostedUrl', ''),
                                    source=self.name,
                                    posted=str(job.get('createdAt', '')),
                                    job_type=categories.get('commitment', ''),
                                ))
                        
                        await asyncio.sleep(0.2)
                        
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
    API: https://www.workingnomads.com/api/exposed_jobs/
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
                    tags = ' '.join(job.get('tags', [])).lower() if job.get('tags') else ''
                    
                    # Filter for relevant jobs
                    is_relevant = any(kw in title or kw in category or kw in tags for kw in keywords_lower)
                    
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


class RemotiveSource(BaseJobSource):
    """
    Remotive API - FREE, NO KEY NEEDED!
    Remote-first jobs API.
    API: https://remotive.com/api/remote-jobs
    """
    name = "Remotive"
    base_url = "https://remotive.com/api/remote-jobs"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=20) as client:
                # Fetch different categories
                categories = ["data", "software-dev", "all-others"]
                
                for category in categories:
                    try:
                        url = f"{self.base_url}?category={category}&limit=100"
                        response = await client.get(url)
                        
                        if response.status_code != 200:
                            continue
                        
                        data = response.json()
                        
                        for job in data.get('jobs', []):
                            title = job.get('title', '').lower()
                            tags = ' '.join(job.get('tags', [])).lower() if job.get('tags') else ''
                            
                            # Filter for relevant jobs
                            is_relevant = any(kw in title or kw in tags for kw in keywords_lower)
                            
                            if is_relevant:
                                jobs.append(RawJob(
                                    title=job.get('title', 'Unknown'),
                                    company=job.get('company_name', 'Unknown'),
                                    location=job.get('candidate_required_location', 'Remote'),
                                    description=job.get('description', '')[:2000],
                                    url=job.get('url', ''),
                                    source=self.name,
                                    posted=job.get('publication_date', ''),
                                    salary=job.get('salary', ''),
                                    job_type=job.get('job_type', ''),
                                ))
                        
                        await asyncio.sleep(0.3)
                    except Exception as e:
                        logger.debug(f"{self.name}: Error for category {category}: {e}")
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


class WeWorkRemotelySource(BaseJobSource):
    """
    We Work Remotely - FREE RSS Feed!
    One of the largest remote job boards.
    """
    name = "WWRemotely"
    
    # RSS feeds for different categories
    RSS_FEEDS = [
        "https://weworkremotely.com/categories/remote-programming-jobs.rss",
        "https://weworkremotely.com/categories/remote-data-jobs.rss",
        "https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss",
        "https://weworkremotely.com/remote-jobs.rss",
    ]
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        seen_ids: Set[str] = set()
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                for feed_url in self.RSS_FEEDS:
                    try:
                        response = await client.get(feed_url)
                        
                        if response.status_code != 200:
                            continue
                        
                        # Parse RSS
                        content = response.text
                        
                        # Simple RSS parsing - handle both CDATA and non-CDATA
                        items = re.findall(r'<item>(.*?)</item>', content, re.DOTALL)
                        
                        for item in items[:30]:
                            # Try CDATA first, then plain
                            title_match = re.search(r'<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>', item)
                            link_match = re.search(r'<link>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</link>', item)
                            pub_date_match = re.search(r'<pubDate>(.*?)</pubDate>', item)
                            desc_match = re.search(r'<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>', item, re.DOTALL)
                            
                            if not title_match:
                                continue
                            
                            title = title_match.group(1).strip()
                            url = link_match.group(1).strip() if link_match else ''
                            posted = pub_date_match.group(1) if pub_date_match else ''
                            description = desc_match.group(1)[:500] if desc_match else ''
                            
                            # Parse company from title (format: "Company: Job Title")
                            parts = title.split(':', 1)
                            company = parts[0].strip() if len(parts) > 1 else 'Unknown'
                            job_title = parts[1].strip() if len(parts) > 1 else title
                            
                            # Check relevance
                            is_relevant = any(kw in title.lower() or kw in description.lower() for kw in keywords_lower)
                            
                            job_key = f"{title}"
                            
                            if is_relevant and job_key not in seen_ids:
                                seen_ids.add(job_key)
                                jobs.append(RawJob(
                                    title=job_title[:100],
                                    company=company[:50],
                                    location="Remote",
                                    description=description,
                                    url=url,
                                    source=self.name,
                                    posted=posted,
                                ))
                        
                        await asyncio.sleep(0.3)
                    except Exception as e:
                        logger.debug(f"{self.name}: Error for feed: {e}")
                        continue
            
            logger.info(f"{self.name}: Found {len(jobs)} jobs (FREE - RSS)")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class JustRemoteSource(BaseJobSource):
    """
    JustRemote - FREE RSS feeds for remote jobs
    """
    name = "JustRemote"
    base_url = "https://justremote.co/api/jobs"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=20) as client:
                # Try API endpoint
                response = await client.get(self.base_url)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        for job in data.get('jobs', data if isinstance(data, list) else []):
                            title = job.get('title', '').lower()
                            
                            is_relevant = any(kw in title for kw in keywords_lower)
                            
                            if is_relevant:
                                jobs.append(RawJob(
                                    title=job.get('title', 'Unknown'),
                                    company=job.get('company', 'Unknown'),
                                    location=job.get('location', 'Remote'),
                                    description=job.get('description', '')[:2000],
                                    url=job.get('url', ''),
                                    source=self.name,
                                    posted=job.get('date', ''),
                                ))
                    except:
                        pass
            
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


class YCJobsSource(BaseJobSource):
    """
    Y Combinator Work at a Startup - Scrape job listings
    Note: Their API requires authentication, so we parse public pages
    """
    name = "YC-Jobs"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                # Use the public job listings page
                try:
                    # Try Algolia API used by the site
                    algolia_url = "https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries"
                    headers = {
                        "x-algolia-api-key": "MjBjYjRiMzY0NzdhZWY0NjExY2NhZjYxMGIxYjc2MTAwNWFkNTkwNTc4NjgxYjU0YzFhYTY2ZGQ5OGY5NDMxZnJlc3RyaWN0SW5kaWNlcz0lNUIlMjJZQ0NvbXBhbnlfcHJvZHVjdGlvbiUyMiUyQyUyMllDQ29tcGFueV9CeV9MYXVuY2hfRGF0ZV9wcm9kdWN0aW9uJTIyJTVEJnRhZ0ZpbHRlcnM9JTVCJTIyeWNkY19wdWJsaWMlMjIlNUQmYW5hbHl0aWNzVGFncz0lNUIlMjJ5Y2RjJTIyJTVE",
                        "x-algolia-application-id": "45BWZJ1SGC",
                    }
                    
                    # Search for data science roles
                    payload = {
                        "requests": [{
                            "indexName": "YCCompany_production",
                            "params": "query=data&hitsPerPage=50"
                        }]
                    }
                    
                    response = await client.post(algolia_url, json=payload, headers=headers)
                    
                    if response.status_code == 200:
                        data = response.json()
                        results = data.get('results', [{}])[0]
                        hits = results.get('hits', [])
                        
                        for company in hits[:30]:
                            company_name = company.get('name', 'Unknown')
                            
                            # Check if hiring and has relevant description
                            if company.get('isHiring'):
                                one_liner = company.get('one_liner', '').lower()
                                
                                is_relevant = any(kw in one_liner for kw in keywords_lower)
                                
                                if is_relevant:
                                    jobs.append(RawJob(
                                        title=f"Data Role at {company_name}",
                                        company=company_name,
                                        location=company.get('location', 'Remote'),
                                        description=company.get('one_liner', '')[:500],
                                        url=f"https://www.ycombinator.com/companies/{company.get('slug', '')}",
                                        source=self.name,
                                        posted="",
                                    ))
                except Exception as e:
                    logger.debug(f"{self.name}: Algolia API error: {e}")
            
            logger.info(f"{self.name}: Found {len(jobs)} YC startup jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(
            source=self.name,
            jobs=jobs,
            fetch_duration_ms=int((time.time() - start) * 1000),
            error=error
        )


class StartupJobsSource(BaseJobSource):
    """
    Startup.jobs - FREE API for startup jobs
    """
    name = "StartupJobs"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=20) as client:
                # Fetch from their public API
                url = "https://startup.jobs/api/jobs?remote=true&page=1"
                response = await client.get(url)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        for job in data.get('jobs', data if isinstance(data, list) else []):
                            title = job.get('title', '').lower()
                            
                            is_relevant = any(kw in title for kw in keywords_lower)
                            
                            if is_relevant:
                                jobs.append(RawJob(
                                    title=job.get('title', 'Unknown'),
                                    company=job.get('company', {}).get('name', 'Unknown') if isinstance(job.get('company'), dict) else str(job.get('company', 'Unknown')),
                                    location=job.get('location', 'Remote'),
                                    description=job.get('description', '')[:2000],
                                    url=job.get('url', ''),
                                    source=self.name,
                                    posted=job.get('published_at', ''),
                                ))
                    except:
                        pass
            
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


# ============================================================================
# NEW SOURCES - Ashby, Workable, Europe Remote, Crypto, AI Jobs, India Startups
# ============================================================================

class AshbyMultiSource(BaseJobSource):
    """
    Ashby ATS - FREE API (no key needed)
    Many YC startups use Ashby.
    """
    name = "Ashby"
    rate_limit_seconds = 0.3
    
    COMPANIES = [
        "ramp", "notion", "linear", "replit", "vercel",
        "supabase", "retool", "webflow", "deel", "mercury",
        "vanta", "ironclad", "lattice", "ashby", "gem",
        "anyscale", "modal", "replicate", "runway", "stability",
        "figma", "loom", "pitch", "miro", "coda",
    ]
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        seen_ids: Set[str] = set()
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                hour = datetime.now().hour
                start_idx = (hour % 5) * 5
                companies_to_check = self.COMPANIES[start_idx:start_idx+5]
                
                if not companies_to_check:
                    companies_to_check = self.COMPANIES[:5]
                
                logger.info(f"{self.name}: Checking {len(companies_to_check)} companies")
                
                for company in companies_to_check:
                    try:
                        url = f"https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"
                        payload = {
                            "operationName": "ApiJobBoardWithTeams",
                            "variables": {"organizationHostedJobsPageName": company},
                            "query": """query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
                                jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
                                    teams { id name jobs { id title employmentType locationName } }
                                }
                            }"""
                        }
                        response = await client.post(url, json=payload)
                        
                        if response.status_code != 200:
                            continue
                        
                        data = response.json()
                        teams = data.get('data', {}).get('jobBoard', {}).get('teams', [])
                        
                        for team in teams:
                            for job in team.get('jobs', []):
                                title = job.get('title', '').lower()
                                location = job.get('locationName', 'Remote')
                                
                                is_relevant = any(kw in title for kw in keywords_lower)
                                
                                location_lower = location.lower()
                                is_india_remote = any(loc in location_lower for loc in [
                                    'india', 'bangalore', 'remote', 'anywhere', 'global'
                                ])
                                
                                job_key = f"{job.get('title', '')}_{company}"
                                
                                if is_relevant and is_india_remote and job_key not in seen_ids:
                                    seen_ids.add(job_key)
                                    jobs.append(RawJob(
                                        title=job.get('title', 'Unknown'),
                                        company=company.replace('-', ' ').title(),
                                        location=location,
                                        description="",
                                        url=f"https://jobs.ashbyhq.com/{company}/{job.get('id', '')}",
                                        source=self.name,
                                        posted="",
                                        job_type=job.get('employmentType', ''),
                                    ))
                        
                        await asyncio.sleep(0.2)
                        
                    except Exception as e:
                        logger.debug(f"{self.name}: Error {company}: {e}")
                        continue
            
            logger.info(f"{self.name}: Found {len(jobs)} jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(source=self.name, jobs=jobs, fetch_duration_ms=int((time.time() - start) * 1000), error=error)


class EuropeRemoteSource(BaseJobSource):
    """European remote job boards - JustJoin.it"""
    name = "Europe-Remote"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                try:
                    url = "https://justjoin.it/api/offers"
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        for job in data[:100]:
                            title = job.get('title', '').lower()
                            workplace = job.get('workplace_type', '')
                            
                            is_relevant = any(kw in title for kw in keywords_lower)
                            is_remote = workplace.lower() == 'remote'
                            
                            if is_relevant and is_remote:
                                salary = job.get('employment_types', [{}])[0]
                                salary_str = f"{salary.get('from', '')}-{salary.get('to', '')} {salary.get('currency', '')}"
                                
                                jobs.append(RawJob(
                                    title=job.get('title', 'Unknown'),
                                    company=job.get('company_name', 'Unknown'),
                                    location="Remote (Europe)",
                                    description="",
                                    url=f"https://justjoin.it/offers/{job.get('id', '')}",
                                    source=self.name,
                                    posted=job.get('published_at', ''),
                                    salary=salary_str if salary.get('from') else None,
                                ))
                except Exception as e:
                    logger.debug(f"{self.name}: JustJoin.it error: {e}")
            
            logger.info(f"{self.name}: Found {len(jobs)} remote jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(source=self.name, jobs=jobs, fetch_duration_ms=int((time.time() - start) * 1000), error=error)


class CryptoJobsSource(BaseJobSource):
    """Crypto/Web3 job boards - remote-first companies"""
    name = "Crypto-Jobs"
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords] + ['data', 'analyst', 'python', 'ml', 'ai']
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                try:
                    url = "https://web3.career/api/v1/jobs?page=1"
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        for job in data.get('jobs', data if isinstance(data, list) else [])[:50]:
                            title = job.get('title', '').lower()
                            
                            is_relevant = any(kw in title for kw in keywords_lower)
                            
                            if is_relevant:
                                jobs.append(RawJob(
                                    title=job.get('title', 'Unknown'),
                                    company=job.get('company', 'Unknown'),
                                    location="Remote (Web3)",
                                    description=job.get('description', '')[:2000],
                                    url=job.get('url', job.get('apply_url', '')),
                                    source=self.name,
                                    posted=job.get('created_at', ''),
                                ))
                except Exception as e:
                    logger.debug(f"{self.name}: Web3.career error: {e}")
            
            logger.info(f"{self.name}: Found {len(jobs)} crypto/web3 jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(source=self.name, jobs=jobs, fetch_duration_ms=int((time.time() - start) * 1000), error=error)


class IndiaStartupsSource(BaseJobSource):
    """India-specific unicorn startups via Greenhouse"""
    name = "India-Startups"
    
    GREENHOUSE_INDIA = [
        "razorpay", "meesho", "cred", "groww", "zerodha",
        "phonepe", "swiggy", "zomato", "freshworks", "clevertap", 
        "browserstack", "postman", "chargebee", "druva", "icertis",
    ]
    
    async def fetch_jobs(self, keywords: List[str]) -> JobBatch:
        start = time.time()
        jobs = []
        error = None
        keywords_lower = [k.lower() for k in keywords]
        
        try:
            import httpx
            
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                for company in self.GREENHOUSE_INDIA[:8]:
                    try:
                        url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
                        response = await client.get(url)
                        
                        if response.status_code != 200:
                            continue
                        
                        data = response.json()
                        
                        for job in data.get('jobs', []):
                            title = job.get('title', '').lower()
                            location = job.get('location', {}).get('name', '') if isinstance(job.get('location'), dict) else ''
                            
                            is_relevant = any(kw in title for kw in keywords_lower)
                            
                            if is_relevant:
                                jobs.append(RawJob(
                                    title=job.get('title', 'Unknown'),
                                    company=company.title(),
                                    location=location or 'India',
                                    description="",
                                    url=job.get('absolute_url', ''),
                                    source=self.name,
                                    posted=job.get('updated_at', ''),
                                ))
                        
                        await asyncio.sleep(0.3)
                        
                    except Exception as e:
                        logger.debug(f"{self.name}: Error {company}: {e}")
                        continue
            
            logger.info(f"{self.name}: Found {len(jobs)} India startup jobs")
            
        except Exception as e:
            error = str(e)[:200]
            logger.error(f"{self.name}: {error}")
        
        return JobBatch(source=self.name, jobs=jobs, fetch_duration_ms=int((time.time() - start) * 1000), error=error)
