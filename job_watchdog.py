#!/usr/bin/env python3
"""
Job Watchdog - Multi-Source Job Alert System
Fetches jobs from multiple FREE APIs and sends Telegram notifications for matches.

Sources:
- RemoteOK (remote tech jobs)
- Arbeitnow (EU/worldwide jobs)  
- Findwork (dev jobs)
- Himalayas (remote jobs)
- Jobicy (remote jobs)
- TheMuse (various jobs)
- WeWorkRemotely (remote RSS)
- Indeed India (Playwright - fresh jobs!)
- Google Jobs (SerpAPI - last 24h!)  
- Naukri Google (SerpAPI indexed)
- LandingJobs (EU with relocation)
- HN-Hiring (Hacker News monthly)
- DuckDuckGo (meta-search)

Focus: JOB FRESHNESS - prioritizes recently posted jobs
Note: SerpAPI has 250 calls/month limit - used efficiently

Setup:
1. Set environment variables in .env file:
   TELEGRAM_TOKEN=your_bot_token
   TELEGRAM_CHAT_ID=your_chat_id

2. Run: python job_watchdog.py
"""

import os
import time
import hashlib
from datetime import datetime
from typing import List, Dict
import pandas as pd
import requests
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='sklearn')

# Load .env file
load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================

# Your resume profiles for matching
DS_PROFILE = """
Data Science, Machine Learning, Supervised Learning, Unsupervised Learning, Regression, Classification, Clustering, Time Series Forecasting, NLP, Natural Language Processing, Feature Engineering, Model Training, Model Evaluation, Hyperparameter Tuning, Cross Validation, Scikit-learn, PyTorch, Keras, Python, Pandas, NumPy, SQL, R, Exploratory Data Analysis, EDA, Statistical Analysis, Hypothesis Testing, A/B Testing, Probability, Linear Regression, Logistic Regression, Random Forest, Gradient Boosting, XGBoost, Model Deployment, ML Pipelines, ETL Pipelines, Data Engineering, Data Validation, Data Automation, Airflow, APIs, Data Integration, Power BI, Data Visualization, Matplotlib, Seaborn, Dashboards, Business Insights, Decision Support, PostgreSQL, SQL Server, Cloud Computing, AWS, AWS Glue, S3, Lambda, EC2, Git, Version Control, Jupyter Notebook, Prompt Engineering, Generative AI, GenAI, LangChain, LLMs, Fine-tuning, Recommendation Systems, Collaborative Filtering, Fraud Analytics, Predictive Modeling, Demand Forecasting, KPI Analysis, Stakeholder Communication
"""

DA_PROFILE = """
SQL, Advanced SQL, Joins, Subqueries, Window Functions, CTEs, Python, Pandas, NumPy, Data Cleaning, Data Wrangling, Data Transformation, Data Validation, Handling Missing Values, Outlier Detection, Relational Databases, MySQL, PostgreSQL, SQL Server, Exploratory Data Analysis, EDA, Descriptive Statistics, Inferential Statistics, Hypothesis Testing, A/B Testing, Correlation Analysis, Regression Analysis, KPI Definition, Metric Design, Power BI, Tableau, Excel, Advanced Excel, Pivot Tables, Power Query, DAX, Dashboards, Data Visualization, Data Storytelling, Business Requirements Gathering, Stakeholder Management, Cross-functional Collaboration, Business Insights, Trend Analysis, Root Cause Analysis, Performance Analysis, Operational Analytics, ETL, ELT, Data Pipelines, Data Integration, APIs, Automation, Scheduling, Azure Data Factory, ADF, AWS, Azure, GCP, Git, Version Control, Agile, Jira, Documentation, Analytical Thinking, Problem Solving, Data-Driven Decision Making, Communication of Insights, Executive Reporting"""

# Search configurations - each will be searched across all sources
SEARCH_CONFIGS = [
    {"keywords": ["data scientist", "machine learning", "ml engineer", "data science", "deep learning", "nlp", "ai engineer"], 
     "profile": DS_PROFILE, "tag": "Data Science"},
    {"keywords": ["data analyst", "business analyst", "analytics", "data analysis", "sql analyst", "reporting analyst"], 
     "profile": DA_PROFILE, "tag": "Data Analytics"},
    {"keywords": ["power bi", "tableau", "bi developer", "business intelligence", "visualization", "dashboard"], 
     "profile": DA_PROFILE, "tag": "BI Developer"},
]

# Minimum match score to send notification (0-100)
MIN_MATCH_SCORE = 15

# Maximum job age in hours (jobs older than this are filtered out)
MAX_JOB_AGE_HOURS = 12

# Files
CSV_FILE = "notified_jobs.csv"

# Telegram credentials
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# SerpAPI for Google Jobs (250 calls/month limit)
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "85a971523020de8adbb505f043ca26fd3a7e42d14a6f372f8890667dab08fae0")
SERPAPI_CALLS_FILE = "serpapi_usage.txt"
SERPAPI_MONTHLY_LIMIT = 250
SERPAPI_RUN_HOURS = [8, 20]  # Only use SerpAPI at 8 AM and 8 PM to conserve quota (2x/day)

# ============================================================================
# JOB SOURCES - Free APIs
# ============================================================================

def rate_limit(seconds: float = 1.0):
    """Rate limiting between API calls to avoid being blocked"""
    time.sleep(seconds)


def parse_job_age_hours(posted) -> float:
    """
    Parse job posting time and return age in hours.
    Returns float('inf') if unable to parse (will be filtered out).
    """
    if not posted:
        return float('inf')  # Unknown age, filter out
    
    now = datetime.now()
    
    # Handle Unix timestamp (integer like 1770418892)
    if isinstance(posted, (int, float)):
        try:
            posted_dt = datetime.fromtimestamp(posted)
            age_hours = (now - posted_dt).total_seconds() / 3600
            return max(0, age_hours)
        except:
            return float('inf')
    
    # Convert to string
    posted = str(posted).strip()
    if not posted:
        return float('inf')
    
    posted_lower = posted.lower()
    
    # Handle "just now", "just posted", etc.
    if any(x in posted_lower for x in ['just', 'now', 'moment', 'second']):
        return 0
    
    # Handle "X minutes ago"
    if 'minute' in posted_lower:
        try:
            mins = int(''.join(filter(str.isdigit, posted_lower.split('minute')[0])) or '0')
            return mins / 60
        except:
            return 1  # Assume recent
    
    # Handle "X hours ago" or "X hour ago"
    if 'hour' in posted_lower:
        try:
            hours = int(''.join(filter(str.isdigit, posted_lower.split('hour')[0])) or '0')
            return hours
        except:
            return 5  # Assume within range
    
    # Handle "today" (check before "day")
    if 'today' in posted_lower:
        return 3  # Assume posted a few hours ago
    
    # Handle "yesterday" (check before "day")
    if 'yesterday' in posted_lower:
        return 30  # ~1.25 days old
    
    # Handle "X days ago"
    if 'day' in posted_lower and 'ago' in posted_lower:
        try:
            days = int(''.join(filter(str.isdigit, posted_lower.split('day')[0])) or '0')
            return days * 24
        except:
            return float('inf')
    
    # Handle "X weeks ago"
    if 'week' in posted_lower:
        return float('inf')  # Too old
    
    # Handle "X months ago"
    if 'month' in posted_lower:
        return float('inf')  # Too old
    
    # Handle ISO date formats with timezone (2026-02-06T00:00:35+00:00)
    if 'T' in posted and ('+' in posted or 'Z' in posted):
        try:
            # Strip timezone and parse
            date_part = posted.split('+')[0].split('Z')[0]
            if '.' in date_part:
                date_part = date_part.split('.')[0]  # Remove microseconds
            posted_dt = datetime.strptime(date_part, '%Y-%m-%dT%H:%M:%S')
            age_hours = (now - posted_dt).total_seconds() / 3600
            return max(0, age_hours)
        except:
            pass
    
    # Handle simple ISO date (2026-02-06T10:30:00 or 2026-02-06)
    if '-' in posted:
        try:
            if 'T' in posted:
                date_part = posted.split('T')[0]
            else:
                date_part = posted[:10]
            posted_dt = datetime.strptime(date_part, '%Y-%m-%d')
            # Add 12 hours as estimate (posted sometime during that day)
            age_hours = (now - posted_dt).total_seconds() / 3600 - 12
            return max(0, age_hours)
        except:
            pass
    
    # Handle Unix timestamp as string
    if posted.isdigit() and len(posted) >= 10:
        try:
            posted_dt = datetime.fromtimestamp(int(posted))
            age_hours = (now - posted_dt).total_seconds() / 3600
            return max(0, age_hours)
        except:
            pass
    
    # Handle "Recent" or similar
    if 'recent' in posted_lower:
        return 3  # Assume recent
    
    # Unknown format - default to fresh (to not filter out valid jobs)
    return 3  # Be lenient - assume fresh if unknown


def is_job_fresh(job: Dict, max_hours: float = MAX_JOB_AGE_HOURS) -> bool:
    """Check if job is within the freshness threshold"""
    posted = job.get('posted', '')
    age_hours = parse_job_age_hours(posted)
    return age_hours <= max_hours


def get_serpapi_usage() -> int:
    """Get current month's SerpAPI usage count"""
    try:
        if os.path.exists(SERPAPI_CALLS_FILE):
            with open(SERPAPI_CALLS_FILE, 'r') as f:
                data = f.read().strip().split(',')
                month = data[0]
                count = int(data[1])
                # Reset if new month
                current_month = datetime.now().strftime('%Y-%m')
                if month == current_month:
                    return count
        return 0
    except:
        return 0


def increment_serpapi_usage():
    """Increment SerpAPI usage counter"""
    try:
        current_month = datetime.now().strftime('%Y-%m')
        count = get_serpapi_usage() + 1
        with open(SERPAPI_CALLS_FILE, 'w') as f:
            f.write(f"{current_month},{count}")
    except:
        pass


def retry_request(url: str, headers: dict = None, timeout: int = 20, max_retries: int = 2) -> requests.Response:
    """Make HTTP request with retry logic"""
    headers = headers or {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            return response
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))  # Exponential backoff
            else:
                raise e
    return None


class RemoteOKSource:
    """RemoteOK.com - Free JSON API for remote tech jobs"""
    name = "RemoteOK"
    
    def fetch_jobs(self, keywords: List[str], location: str = "") -> List[Dict]:
        jobs = []
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            response = requests.get(
                "https://remoteok.com/api",
                headers=headers, 
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"  ⚠️ RemoteOK: HTTP {response.status_code}")
                return []
            
            data = response.json()
            job_list = data[1:] if len(data) > 1 else []
            
            for job in job_list:
                job_text = f"{job.get('position', '')} {job.get('description', '')} {' '.join(job.get('tags', []))}".lower()
                
                if any(kw.lower() in job_text for kw in keywords):
                    jobs.append({
                        'title': job.get('position', 'Unknown'),
                        'company': job.get('company', 'Unknown'),
                        'location': job.get('location', 'Remote'),
                        'description': job.get('description', '')[:2000],
                        'url': job.get('url', f"https://remoteok.com"),
                        'source': self.name,
                        'posted': job.get('date', '')
                    })
            
            print(f"  ✓ RemoteOK: {len(jobs)} matches from {len(job_list)} jobs")
            
        except Exception as e:
            print(f"  ⚠️ RemoteOK error: {str(e)[:80]}")
        
        return jobs


class ArbeitnowSource:
    """Arbeitnow.com - Free JSON API"""
    name = "Arbeitnow"
    
    def fetch_jobs(self, keywords: List[str], location: str = "") -> List[Dict]:
        jobs = []
        try:
            headers = {"User-Agent": "JobWatchdog/1.0"}
            response = requests.get(
                "https://www.arbeitnow.com/api/job-board-api",
                headers=headers, 
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"  ⚠️ Arbeitnow: HTTP {response.status_code}")
                return []
            
            data = response.json()
            job_list = data.get('data', [])
            
            for job in job_list:
                job_text = f"{job.get('title', '')} {job.get('description', '')} {' '.join(job.get('tags', []))}".lower()
                
                if any(kw.lower() in job_text for kw in keywords):
                    jobs.append({
                        'title': job.get('title', 'Unknown'),
                        'company': job.get('company_name', 'Unknown'),
                        'location': job.get('location', 'Unknown'),
                        'description': job.get('description', '')[:2000],
                        'url': job.get('url', ''),
                        'source': self.name,
                        'posted': job.get('created_at', '')
                    })
            
            print(f"  ✓ Arbeitnow: {len(jobs)} matches from {len(job_list)} jobs")
            
        except Exception as e:
            print(f"  ⚠️ Arbeitnow error: {str(e)[:80]}")
        
        return jobs


class FindworkSource:
    """Findwork.dev - Free API for dev jobs"""
    name = "Findwork"
    
    def fetch_jobs(self, keywords: List[str], location: str = "") -> List[Dict]:
        jobs = []
        try:
            headers = {"User-Agent": "JobWatchdog/1.0"}
            
            for keyword in keywords[:2]:
                params = {"search": keyword}
                
                response = requests.get(
                    "https://findwork.dev/api/jobs/",
                    headers=headers, 
                    params=params, 
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    for job in data.get('results', []):
                        jobs.append({
                            'title': job.get('role', 'Unknown'),
                            'company': job.get('company_name', 'Unknown'),
                            'location': job.get('location', 'Unknown'),
                            'description': job.get('text', '')[:2000],
                            'url': job.get('url', ''),
                            'source': self.name,
                            'posted': job.get('date_posted', '')
                        })
                
                rate_limit()
            
            seen = set()
            unique_jobs = []
            for job in jobs:
                key = f"{job['title']}_{job['company']}"
                if key not in seen:
                    seen.add(key)
                    unique_jobs.append(job)
            
            print(f"  ✓ Findwork: {len(unique_jobs)} matches")
            return unique_jobs
            
        except Exception as e:
            print(f"  ⚠️ Findwork error: {str(e)[:80]}")
        
        return jobs


class HimalayasSource:
    """Himalayas.app - Free API for remote jobs"""
    name = "Himalayas"
    
    def fetch_jobs(self, keywords: List[str], location: str = "") -> List[Dict]:
        jobs = []
        try:
            headers = {"User-Agent": "JobWatchdog/1.0"}
            response = requests.get(
                "https://himalayas.app/jobs/api",
                headers=headers, 
                params={"limit": 100},
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"  ⚠️ Himalayas: HTTP {response.status_code}")
                return []
            
            data = response.json()
            job_list = data.get('jobs', [])
            
            for job in job_list:
                categories = job.get('categories', [])
                if isinstance(categories, list):
                    categories = ' '.join(categories)
                
                job_text = f"{job.get('title', '')} {job.get('description', '')} {categories}".lower()
                
                if any(kw.lower() in job_text for kw in keywords):
                    jobs.append({
                        'title': job.get('title', 'Unknown'),
                        'company': job.get('companyName', 'Unknown'),
                        'location': ', '.join(job.get('locationRestrictions', [])) or 'Remote',
                        'description': job.get('description', '')[:2000],
                        'url': job.get('applicationLink', '') or f"https://himalayas.app/jobs/{job.get('slug', '')}",
                        'source': self.name,
                        'posted': job.get('pubDate', '')
                    })
            
            print(f"  ✓ Himalayas: {len(jobs)} matches from {len(job_list)} jobs")
            
        except Exception as e:
            print(f"  ⚠️ Himalayas error: {str(e)[:80]}")
        
        return jobs


class JobicySource:
    """Jobicy.com - Free API for remote jobs"""
    name = "Jobicy"
    
    def fetch_jobs(self, keywords: List[str], location: str = "") -> List[Dict]:
        jobs = []
        try:
            headers = {"User-Agent": "JobWatchdog/1.0"}
            
            industries = ["data-science", "software-dev", "all"]
            
            for industry in industries:
                params = {"count": 50}
                if industry != "all":
                    params["industry"] = industry
                
                response = requests.get(
                    "https://jobicy.com/api/v2/remote-jobs",
                    headers=headers, 
                    params=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    job_list = data.get('jobs', [])
                    
                    for job in job_list:
                        job_text = f"{job.get('jobTitle', '')} {job.get('jobExcerpt', '')} {job.get('jobIndustry', '')}".lower()
                        
                        if any(kw.lower() in job_text for kw in keywords):
                            jobs.append({
                                'title': job.get('jobTitle', 'Unknown'),
                                'company': job.get('companyName', 'Unknown'),
                                'location': job.get('jobGeo', 'Remote'),
                                'description': job.get('jobExcerpt', '')[:2000],
                                'url': job.get('url', ''),
                                'source': self.name,
                                'posted': job.get('pubDate', '')
                            })
                
                rate_limit()
            
            seen = set()
            unique_jobs = []
            for job in jobs:
                key = f"{job['title']}_{job['company']}"
                if key not in seen:
                    seen.add(key)
                    unique_jobs.append(job)
            
            print(f"  ✓ Jobicy: {len(unique_jobs)} matches")
            return unique_jobs
            
        except Exception as e:
            print(f"  ⚠️ Jobicy error: {str(e)[:80]}")
        
        return jobs


class TheMuseSource:
    """TheMuse.com - Free API"""
    name = "TheMuse"
    
    def fetch_jobs(self, keywords: List[str], location: str = "") -> List[Dict]:
        jobs = []
        try:
            headers = {"User-Agent": "JobWatchdog/1.0"}
            
            categories = ["Data%20Science", "Data%20and%20Analytics", "Software%20Engineering"]
            
            for category in categories:
                response = requests.get(
                    f"https://www.themuse.com/api/public/jobs?category={category}&page=1",
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    for job in data.get('results', []):
                        job_text = f"{job.get('name', '')} {job.get('contents', '')}".lower()
                        
                        if any(kw.lower() in job_text for kw in keywords):
                            company = job.get('company', {})
                            locations = job.get('locations', [{}])
                            
                            jobs.append({
                                'title': job.get('name', 'Unknown'),
                                'company': company.get('name', 'Unknown') if isinstance(company, dict) else 'Unknown',
                                'location': locations[0].get('name', 'Unknown') if locations else 'Unknown',
                                'description': job.get('contents', '')[:2000],
                                'url': job.get('refs', {}).get('landing_page', ''),
                                'source': self.name,
                                'posted': job.get('publication_date', '')
                            })
                
                rate_limit()
            
            seen = set()
            unique_jobs = []
            for job in jobs:
                key = f"{job['title']}_{job['company']}"
                if key not in seen:
                    seen.add(key)
                    unique_jobs.append(job)
            
            print(f"  ✓ TheMuse: {len(unique_jobs)} matches")
            return unique_jobs
            
        except Exception as e:
            print(f"  ⚠️ TheMuse error: {str(e)[:80]}")
        
        return jobs


class WeWorkRemotelySource:
    """WeWorkRemotely.com - Remote job board with RSS feeds"""
    name = "WWRemotely"
    
    def fetch_jobs(self, keywords: List[str], location: str = "") -> List[Dict]:
        jobs = []
        try:
            import re
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            
            # RSS feeds for different categories
            rss_feeds = [
                "https://weworkremotely.com/categories/remote-programming-jobs.rss",
                "https://weworkremotely.com/categories/remote-data-jobs.rss",
                "https://weworkremotely.com/categories/remote-full-stack-programming-jobs.rss",
            ]
            
            for feed_url in rss_feeds:
                response = requests.get(feed_url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    # Parse RSS XML
                    items = re.findall(r'<item>(.*?)</item>', response.text, re.DOTALL)
                    
                    for item in items:
                        # Extract fields
                        title_match = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>', item)
                        link_match = re.search(r'<link>(.*?)</link>', item)
                        desc_match = re.search(r'<description><!\[CDATA\[(.*?)\]\]></description>', item, re.DOTALL)
                        pubdate_match = re.search(r'<pubDate>(.*?)</pubDate>', item)
                        
                        title = title_match.group(1) if title_match else None
                        
                        if title:
                            # Check keyword match
                            title_lower = title.lower()
                            if any(kw.lower() in title_lower for kw in keywords):
                                # Extract company from title (format: "Company: Job Title")
                                parts = title.split(':', 1)
                                company = parts[0].strip() if len(parts) > 1 else 'Unknown'
                                job_title = parts[1].strip() if len(parts) > 1 else title
                                
                                jobs.append({
                                    'title': job_title,
                                    'company': company,
                                    'location': 'Remote',
                                    'description': re.sub(r'<[^>]+>', '', desc_match.group(1))[:2000] if desc_match else '',
                                    'url': link_match.group(1) if link_match else '',
                                    'source': self.name,
                                    'posted': pubdate_match.group(1) if pubdate_match else ''
                                })
                
                rate_limit()
            
            # Dedupe
            seen = set()
            unique_jobs = []
            for job in jobs:
                key = f"{job['title']}_{job['company']}"
                if key not in seen:
                    seen.add(key)
                    unique_jobs.append(job)
            
            print(f"  ✓ WWRemotely: {len(unique_jobs)} matches")
            return unique_jobs
            
        except Exception as e:
            print(f"  ⚠️ WWRemotely error: {str(e)[:80]}")
        
        return jobs


class BingJobSearchSource:
    """Search for jobs via Bing - aggregates from LinkedIn, Naukri, Indeed, Glassdoor"""
    name = "BingSearch"
    
    def fetch_jobs(self, keywords: List[str], location: str = "india") -> List[Dict]:
        jobs = []
        import re
        
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
            
            # Search across multiple job sites via Bing
            sites_to_search = [
                ("LinkedIn", "site:linkedin.com/jobs"),
                ("Naukri", "site:naukri.com"),
                ("Indeed", "site:indeed.com OR site:in.indeed.com"),
                ("Glassdoor", "site:glassdoor.com OR site:glassdoor.co.in"),
            ]
            
            for site_name, site_query in sites_to_search:
                for keyword in keywords[:2]:
                    search_query = f"{site_query} {keyword} {location}"
                    url = f"https://www.bing.com/search?q={requests.utils.quote(search_query)}&count=20"
                    
                    response = requests.get(url, headers=headers, timeout=20)
                    
                    if response.status_code == 200:
                        html = response.text
                        
                        # Extract search results
                        # Bing result pattern: <li class="b_algo">...<h2><a href="URL">TITLE</a></h2>...
                        results = re.findall(
                            r'<li class="b_algo"[^>]*>.*?<h2><a href="([^"]+)"[^>]*>([^<]+)</a>.*?<p[^>]*>([^<]*)</p>',
                            html, re.DOTALL
                        )
                        
                        for job_url, title, snippet in results[:5]:
                            # Clean title
                            title = re.sub(r'<[^>]+>', '', title).strip()
                            snippet = re.sub(r'<[^>]+>', '', snippet).strip()
                            
                            # Skip non-job URLs
                            if '/jobs/' not in job_url.lower() and 'job' not in job_url.lower():
                                continue
                            
                            # Extract company from title if possible
                            company = 'Unknown'
                            if ' at ' in title:
                                parts = title.split(' at ')
                                title = parts[0].strip()
                                company = parts[1].strip() if len(parts) > 1 else 'Unknown'
                            elif ' - ' in title:
                                parts = title.split(' - ')
                                if len(parts) >= 2:
                                    title = parts[0].strip()
                                    company = parts[1].strip()
                            
                            jobs.append({
                                'title': title[:100],
                                'company': company[:50],
                                'location': location.title(),
                                'description': snippet[:500],
                                'url': job_url,
                                'source': f"Bing→{site_name}",
                                'posted': ''
                            })
                    
                    rate_limit()
            
            # Dedupe
            seen = set()
            unique_jobs = []
            for job in jobs:
                key = f"{job['title']}_{job['company']}"
                if key not in seen:
                    seen.add(key)
                    unique_jobs.append(job)
            
            print(f"  ✓ BingSearch: {len(unique_jobs)} jobs (LinkedIn+Naukri+Indeed+Glassdoor)")
            return unique_jobs
            
        except Exception as e:
            print(f"  ⚠️ BingSearch error: {str(e)[:80]}")
        
        return jobs


class DuckDuckGoJobSource:
    """DuckDuckGo HTML search for jobs"""
    name = "DuckDuckGo"
    
    def fetch_jobs(self, keywords: List[str], location: str = "india") -> List[Dict]:
        jobs = []
        import re
        
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }
            
            for keyword in keywords[:2]:
                search_query = f"{keyword} jobs {location} hiring"
                url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(search_query)}"
                
                response = requests.get(url, headers=headers, timeout=20)
                
                if response.status_code == 200:
                    html = response.text
                    
                    # Extract results from DuckDuckGo HTML
                    # Pattern: class="result__url"...href="URL"...class="result__title"
                    results = re.findall(
                        r'<a[^>]*class="result__url"[^>]*href="([^"]+)"[^>]*>.*?<a[^>]*class="result__a"[^>]*>([^<]+)</a>.*?<a[^>]*class="result__snippet"[^>]*>([^<]*)</a>',
                        html, re.DOTALL
                    )
                    
                    if not results:
                        # Alternative pattern
                        results = re.findall(
                            r'href="//duckduckgo\.com/l/\?uddg=([^&]+)[^"]*"[^>]*>([^<]+)</a>',
                            html
                        )
                        for url_encoded, title in results[:8]:
                            import urllib.parse
                            job_url = urllib.parse.unquote(url_encoded)
                            
                            if any(site in job_url.lower() for site in ['linkedin', 'naukri', 'indeed', 'glassdoor', 'monster']):
                                jobs.append({
                                    'title': title[:100],
                                    'company': 'Unknown',
                                    'location': location.title(),
                                    'description': title,
                                    'url': job_url,
                                    'source': self.name,
                                    'posted': ''
                                })
                
                rate_limit()
            
            # Dedupe
            seen = set()
            unique_jobs = []
            for job in jobs:
                key = job['url']
                if key not in seen:
                    seen.add(key)
                    unique_jobs.append(job)
            
            print(f"  ✓ DuckDuckGo: {len(unique_jobs)} jobs")
            return unique_jobs
            
        except Exception as e:
            print(f"  ⚠️ DuckDuckGo error: {str(e)[:80]}")
        
        return jobs


class GoogleJobsSource:
    """Google Jobs via SerpAPI - Fresh jobs from Google (250 calls/month limit)"""
    name = "Google-Jobs"
    
    def fetch_jobs(self, keywords: List[str], location: str = "india") -> List[Dict]:
        jobs = []
        
        # Check API usage limit
        current_usage = get_serpapi_usage()
        if current_usage >= SERPAPI_MONTHLY_LIMIT:
            print(f"  ⚠️ Google-Jobs: Monthly limit reached ({current_usage}/{SERPAPI_MONTHLY_LIMIT})")
            return jobs
        
        try:
            from serpapi import GoogleSearch
            
            # Only use 1 API call per run to conserve quota
            keyword = keywords[0] if keywords else "data scientist"
            
            # Use Google Jobs engine for best results
            params = {
                "engine": "google_jobs",
                "q": f"{keyword} jobs posted within last 24 hours",
                "location": "India",
                "google_domain": "google.co.in",
                "hl": "en",
                "gl": "in",
                "api_key": SERPAPI_KEY
            }
            
            search = GoogleSearch(params)
            results = search.get_dict()
            
            # Track API usage
            increment_serpapi_usage()
            new_usage = get_serpapi_usage()
            
            # Parse job results
            jobs_results = results.get('jobs_results', [])
            
            for job in jobs_results[:20]:
                title = job.get('title', '')
                company = job.get('company_name', 'Unknown')
                loc = job.get('location', 'India')
                description = job.get('description', '')[:500]
                
                # Get apply link or job link
                apply_options = job.get('apply_options', [])
                url = apply_options[0].get('link', '') if apply_options else job.get('share_link', '')
                
                # Get posted time
                detected_extensions = job.get('detected_extensions', {})
                posted = detected_extensions.get('posted_at', '')
                
                jobs.append({
                    'title': title[:100],
                    'company': company[:50],
                    'location': loc,
                    'description': description,
                    'url': url,
                    'source': self.name,
                    'posted': posted
                })
            
            print(f"  ✓ Google-Jobs: {len(jobs)} fresh jobs (API: {new_usage}/{SERPAPI_MONTHLY_LIMIT})")
            
        except ImportError:
            print(f"  ⚠️ Google-Jobs: serpapi not installed (pip install google-search-results)")
        except Exception as e:
            print(f"  ⚠️ Google-Jobs error: {str(e)[:80]}")
        
        return jobs


class NaukriGoogleSource:
    """Naukri jobs via Google SerpAPI (indexed jobs)"""
    name = "Naukri-Google"
    
    def fetch_jobs(self, keywords: List[str], location: str = "india") -> List[Dict]:
        jobs = []
        
        # Check API usage limit
        current_usage = get_serpapi_usage()
        if current_usage >= SERPAPI_MONTHLY_LIMIT:
            print(f"  ⚠️ Naukri-Google: Monthly limit reached ({current_usage}/{SERPAPI_MONTHLY_LIMIT})")
            return jobs
        
        try:
            from serpapi import GoogleSearch
            import re
            
            keyword = keywords[0] if keywords else "data scientist"
            
            # Search for Naukri listings on Google
            params = {
                "engine": "google",
                "q": f"site:naukri.com {keyword} jobs India",
                "location": "India",
                "google_domain": "google.co.in",
                "hl": "en",
                "gl": "in",
                "num": 20,
                "tbs": "qdr:d",  # Last 24 hours
                "api_key": SERPAPI_KEY
            }
            
            search = GoogleSearch(params)
            results = search.get_dict()
            
            # Track API usage
            increment_serpapi_usage()
            new_usage = get_serpapi_usage()
            
            # Parse organic results
            organic_results = results.get('organic_results', [])
            
            for result in organic_results:
                url = result.get('link', '')
                if 'naukri.com' in url:
                    title = result.get('title', '')
                    snippet = result.get('snippet', '')
                    
                    # Clean up title
                    title = re.sub(r'\s*-\s*Naukri\.com.*$', '', title)
                    title = re.sub(r'\s*\|.*$', '', title)
                    
                    # Extract company if in title
                    company = 'Naukri Listing'
                    if ' - ' in title:
                        parts = title.rsplit(' - ', 1)
                        if len(parts) == 2 and len(parts[1]) < 40:
                            title = parts[0].strip()
                            company = parts[1].strip()
                    
                    jobs.append({
                        'title': title[:100],
                        'company': company[:50],
                        'location': 'India',
                        'description': snippet[:300],
                        'url': url,
                        'source': self.name,
                        'posted': 'Recent'
                    })
            
            print(f"  ✓ Naukri-Google: {len(jobs)} indexed jobs (API: {new_usage}/{SERPAPI_MONTHLY_LIMIT})")
            
        except ImportError:
            print(f"  ⚠️ Naukri-Google: serpapi not installed")
        except Exception as e:
            print(f"  ⚠️ Naukri-Google error: {str(e)[:80]}")
        
        return jobs


class IndeedIndiaSource:
    """Indeed India - Direct scraping with Playwright (fresh jobs)"""
    name = "Indeed-India"
    
    def fetch_jobs(self, keywords: List[str], location: str = "india") -> List[Dict]:
        jobs = []
        
        try:
            import asyncio
            from playwright.async_api import async_playwright
            from playwright_stealth import Stealth
            
            async def scrape_indeed():
                nonlocal jobs
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                    )
                    page = await context.new_page()
                    stealth = Stealth()
                    await stealth.apply_stealth_async(page)
                    
                    for keyword in keywords[:2]:
                        try:
                            # fromage=3 = last 3 days, sort=date for freshness
                            search_url = f"https://in.indeed.com/jobs?q={requests.utils.quote(keyword)}&l={location}&sort=date&fromage=3"
                            await page.goto(search_url, timeout=30000)
                            await page.wait_for_timeout(2000)
                            
                            job_cards = await page.query_selector_all(".job_seen_beacon, [data-jk]")
                            
                            for card in job_cards[:15]:
                                try:
                                    title_el = await card.query_selector("h2.jobTitle a, .jobTitle a, h2 a")
                                    company_el = await card.query_selector("[data-testid='company-name'], .companyName")
                                    location_el = await card.query_selector("[data-testid='text-location'], .companyLocation")
                                    date_el = await card.query_selector(".date, .result-footer, [data-testid='myJobsStateDate'], .new, .job-snippet-footer")
                                    
                                    title = await title_el.inner_text() if title_el else "Unknown"
                                    company = await company_el.inner_text() if company_el else "Unknown"
                                    loc = await location_el.inner_text() if location_el else "India"
                                    posted = await date_el.inner_text() if date_el else "today"  # Default to today since fromage=3
                                    
                                    href = await title_el.get_attribute("href") if title_el else ""
                                    url = f"https://in.indeed.com{href}" if href and not href.startswith("http") else href
                                    
                                    if title and title != "Unknown":
                                        jobs.append({
                                            'title': title[:100],
                                            'company': company[:50] if company else 'Unknown',
                                            'location': loc,
                                            'description': f"{title} at {company}",
                                            'url': url,
                                            'source': self.name,
                                            'posted': posted
                                        })
                                except:
                                    continue
                        except:
                            continue
                    
                    await browser.close()
            
            # Run async function
            asyncio.run(scrape_indeed())
            
            # Dedupe
            seen = set()
            unique_jobs = []
            for job in jobs:
                key = f"{job['title']}_{job['company']}"
                if key not in seen:
                    seen.add(key)
                    unique_jobs.append(job)
            
            print(f"  ✓ Indeed-India: {len(unique_jobs)} fresh jobs")
            return unique_jobs
            
        except Exception as e:
            print(f"  ⚠️ Indeed-India error: {str(e)[:80]}")
        
        return jobs


class NaukriSearchSource:
    """Naukri jobs via search engine indexing (Google Cache/Wayback)"""
    name = "Naukri-Index"
    
    def fetch_jobs(self, keywords: List[str], location: str = "india") -> List[Dict]:
        jobs = []
        import re
        
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            for keyword in keywords[:2]:
                # Try Yandex (Russian search engine - less restrictive)
                search_query = f"site:naukri.com {keyword} jobs"
                url = f"https://yandex.com/search/?text={requests.utils.quote(search_query)}&lr=213"
                
                try:
                    response = requests.get(url, headers=headers, timeout=15)
                    
                    if response.status_code == 200:
                        html = response.text
                        
                        # Extract Naukri URLs from search results
                        naukri_links = re.findall(r'href="(https?://www\.naukri\.com/job-listings[^"]+)"', html)
                        titles = re.findall(r'<a[^>]*href="https?://www\.naukri\.com/job-listings[^"]*"[^>]*>([^<]+)</a>', html)
                        
                        for i, job_url in enumerate(naukri_links[:10]):
                            title = titles[i] if i < len(titles) else f"{keyword} position"
                            title = re.sub(r'<[^>]+>', '', title).strip()
                            
                            jobs.append({
                                'title': title[:100],
                                'company': 'Naukri Listing',
                                'location': 'India',
                                'description': title,
                                'url': job_url,
                                'source': self.name,
                                'posted': ''
                            })
                except:
                    pass
                
                rate_limit()
            
            # Dedupe
            seen = set()
            unique_jobs = []
            for job in jobs:
                if job['url'] not in seen:
                    seen.add(job['url'])
                    unique_jobs.append(job)
            
            print(f"  ✓ Naukri-Index: {len(unique_jobs)} jobs")
            return unique_jobs
            
        except Exception as e:
            print(f"  ⚠️ Naukri-Index error: {str(e)[:80]}")
        
        return jobs


class LandingJobsSource:
    """Landing.jobs - EU tech jobs with relocation support"""
    name = "LandingJobs"
    
    def fetch_jobs(self, keywords: List[str], location: str = "") -> List[Dict]:
        jobs = []
        
        try:
            headers = {
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            response = requests.get(
                "https://landing.jobs/api/v1/jobs?page=1&per_page=50",
                headers=headers,
                timeout=20
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list):
                    job_list = data
                else:
                    job_list = data.get('jobs', data.get('data', []))
                
                keyword_lower = [k.lower() for k in keywords]
                
                for job_data in job_list:
                    title = job_data.get('title', '').lower()
                    description = job_data.get('description', '').lower()
                    skills = ' '.join(job_data.get('skills', [])).lower() if isinstance(job_data.get('skills'), list) else ''
                    
                    searchable = f"{title} {description} {skills}"
                    
                    if any(kw in searchable for kw in keyword_lower):
                        company_name = job_data.get('company', {}).get('name', 'Unknown') if isinstance(job_data.get('company'), dict) else job_data.get('company_name', 'Unknown')
                        
                        jobs.append({
                            'title': job_data.get('title', '')[:100],
                            'company': str(company_name)[:50],
                            'location': job_data.get('city', job_data.get('location', 'Remote')),
                            'description': f"{title} {skills}",
                            'url': job_data.get('url', job_data.get('apply_url', '')),
                            'source': self.name,
                            'posted': job_data.get('created_at', '')
                        })
                        
                        if len(jobs) >= 25:
                            break
                
                print(f"  ✓ LandingJobs: {len(jobs)} jobs")
            
        except Exception as e:
            print(f"  ⚠️ LandingJobs error: {str(e)[:80]}")
        
        return jobs


class HNHiringSource:
    """Hacker News 'Who is Hiring' monthly threads - quality tech jobs"""
    name = "HN-Hiring"
    
    def fetch_jobs(self, keywords: List[str], location: str = "") -> List[Dict]:
        jobs = []
        import re
        
        try:
            # Find latest "Who is hiring" thread
            response = requests.get(
                "https://hn.algolia.com/api/v1/search?query=who%20is%20hiring&tags=story&hitsPerPage=5",
                timeout=20
            )
            
            if response.status_code != 200:
                return jobs
            
            data = response.json()
            hits = data.get('hits', [])
            
            # Find the most recent hiring thread
            thread_id = None
            for hit in hits:
                title = hit.get('title', '')
                if 'Who is hiring' in title or "Who's hiring" in title:
                    thread_id = hit.get('objectID')
                    break
            
            if not thread_id:
                return jobs
            
            # Get job comments from this thread
            response2 = requests.get(
                f"https://hn.algolia.com/api/v1/search?tags=comment,story_{thread_id}&hitsPerPage=100",
                timeout=20
            )
            
            if response2.status_code != 200:
                return jobs
            
            comments = response2.json().get('hits', [])
            keyword_lower = [k.lower() for k in keywords]
            
            for comment in comments:
                text = comment.get('comment_text', '')
                if not text:
                    continue
                
                # Clean HTML
                text_clean = re.sub(r'<[^>]+>', ' ', text)
                text_lower = text_clean.lower()
                
                if any(kw in text_lower for kw in keyword_lower):
                    # Extract company name (usually first line or contains "|")
                    lines = text_clean.strip().split('\n')
                    first_line = lines[0] if lines else ''
                    
                    # Try to extract company from first line
                    company = 'HN Company'
                    if '|' in first_line:
                        parts = first_line.split('|')
                        company = parts[0].strip()[:50]
                    elif len(first_line) < 60:
                        company = first_line[:50]
                    
                    # Extract title hint
                    title = 'Tech Position'
                    if 'engineer' in text_lower:
                        title = 'Software Engineer'
                    elif 'data scientist' in text_lower:
                        title = 'Data Scientist'
                    elif 'analyst' in text_lower:
                        title = 'Data Analyst'
                    elif 'ml' in text_lower or 'machine learning' in text_lower:
                        title = 'ML Engineer'
                    elif 'python' in text_lower:
                        title = 'Python Developer'
                    
                    jobs.append({
                        'title': title,
                        'company': company,
                        'location': 'Remote/Various',
                        'description': text_clean[:500],
                        'url': f"https://news.ycombinator.com/item?id={comment.get('objectID', '')}",
                        'source': self.name,
                        'posted': comment.get('created_at', '')
                    })
                    
                    if len(jobs) >= 20:
                        break
            
            print(f"  ✓ HN-Hiring: {len(jobs)} jobs")
            
        except Exception as e:
            print(f"  ⚠️ HN-Hiring error: {str(e)[:80]}")
        
        return jobs


class GitHubJobsSource:
    """GitHub public job boards and repos aggregating jobs"""
    name = "GitHub-Jobs"
    
    def fetch_jobs(self, keywords: List[str], location: str = "") -> List[Dict]:
        jobs = []
        
        try:
            headers = {
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "Mozilla/5.0"
            }
            
            # Search for jobs in public job listing repos
            for keyword in keywords[:2]:
                query = f"{keyword} job hiring in:file language:markdown"
                response = requests.get(
                    f"https://api.github.com/search/code?q={requests.utils.quote(query)}&per_page=10",
                    headers=headers,
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    items = data.get('items', [])
                    
                    for item in items[:5]:
                        repo = item.get('repository', {})
                        jobs.append({
                            'title': f"{keyword.title()} position",
                            'company': repo.get('full_name', 'GitHub Listing'),
                            'location': 'Remote/Various',
                            'description': f"Job listing found on GitHub: {item.get('path', '')}",
                            'url': item.get('html_url', ''),
                            'source': self.name,
                            'posted': ''
                        })
                
                rate_limit()
            
            # Dedupe
            seen = set()
            unique_jobs = []
            for job in jobs:
                if job['url'] not in seen:
                    seen.add(job['url'])
                    unique_jobs.append(job)
            
            print(f"  ✓ GitHub-Jobs: {len(unique_jobs)} jobs")
            return unique_jobs
            
        except Exception as e:
            print(f"  ⚠️ GitHub-Jobs error: {str(e)[:80]}")
        
        return jobs


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_job_id(job: Dict) -> str:
    """Generate unique ID for a job"""
    text = f"{job['title']}_{job['company']}_{job['source']}"
    return hashlib.md5(text.encode()).hexdigest()[:16]


def get_match_score(resume: str, job: Dict) -> float:
    """Calculate match score using hybrid approach (keywords + TF-IDF)"""
    job_text = f"{job['title']} {job.get('company', '')} {job.get('description', '')}".lower()
    
    if not job_text or len(job_text.strip()) < 50:
        return 0.0
    
    # Key skills to match (weighted)
    key_skills = {
        # Data Science skills
        'python': 8, 'machine learning': 10, 'deep learning': 10,
        'nlp': 8, 'pytorch': 8, 'tensorflow': 8, 'keras': 7,
        'xgboost': 7, 'scikit-learn': 7, 'sklearn': 7,
        'data scientist': 10, 'ml engineer': 10,
        'aws': 6, 'airflow': 6, 'etl': 5, 'sql': 6,
        'bert': 7, 'transformer': 7, 'neural network': 8,
        'time series': 7, 'arima': 6, 'forecasting': 6,
        
        # Data Analyst skills  
        'power bi': 10, 'tableau': 9, 'data analyst': 10,
        'dax': 8, 'power query': 7, 'excel': 5,
        'sql server': 7, 'postgresql': 6, 'mysql': 6,
        'data visualization': 8, 'business intelligence': 8,
        'bi developer': 9, 'analytics': 7,
        'dashboard': 6, 'reporting': 5,
    }
    
    # Calculate keyword score
    keyword_score = 0
    max_keyword_score = 50  # Cap at 50 points from keywords
    
    for skill, weight in key_skills.items():
        if skill in job_text:
            keyword_score += weight
    
    keyword_score = min(keyword_score, max_keyword_score)
    
    # Calculate TF-IDF score
    tfidf_score = 0.0
    try:
        vectorizer = TfidfVectorizer(
            stop_words='english',
            ngram_range=(1, 2),
            max_features=3000,
            min_df=1
        )
        matrix = vectorizer.fit_transform([resume.lower(), job_text])
        tfidf_score = cosine_similarity(matrix)[0][1] * 50  # Scale to 0-50
    except Exception:
        pass
    
    # Combine scores (keyword + tfidf)
    total_score = keyword_score + tfidf_score
    
    return round(min(total_score, 100), 2)


def send_telegram(text: str) -> bool:
    """Send Telegram notification"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        response = requests.post(
            url,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "false"
            },
            timeout=15
        )
        return response.status_code == 200
    except Exception as e:
        print(f"⚠️ Telegram error: {str(e)[:80]}")
        return False


def test_telegram() -> bool:
    """Test Telegram bot connection"""
    if not TELEGRAM_TOKEN:
        print("❌ TELEGRAM_TOKEN not set in .env file")
        print("   Get your bot token from @BotFather on Telegram")
        return False
    if not TELEGRAM_CHAT_ID:
        print("❌ TELEGRAM_CHAT_ID not set in .env file")
        print("   To get your chat ID:")
        print("   1. Send any message to your bot on Telegram")
        print("   2. Visit this URL to get your chat_id:")
        print(f"   https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates")
        print("   3. Look for 'chat':{'id': YOUR_CHAT_ID}")
        return False
    
    try:
        response = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe",
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            if data.get('ok'):
                print(f"✅ Telegram connected: @{data['result']['username']}")
                return True
            else:
                print(f"❌ Telegram API error: {data.get('description', 'Unknown error')}")
        else:
            print(f"❌ Telegram verification failed (HTTP {response.status_code})")
        return False
    except Exception as e:
        print(f"❌ Telegram error: {e}")
        return False


def load_history() -> set:
    """Load previously notified job IDs"""
    history = set()
    if os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0:
        try:
            df = pd.read_csv(CSV_FILE)
            if 'id' in df.columns:
                history = set(df['id'].astype(str))
        except:
            pass
    return history


def save_history(history: set):
    """Save notified job IDs"""
    try:
        pd.DataFrame({'id': list(history)}).to_csv(CSV_FILE, index=False)
    except:
        pass


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 70)
    print("🤖 JOB WATCHDOG - Multi-Source Job Scanner")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Test Telegram
    telegram_ok = test_telegram()
    
    # Load history
    history = load_history()
    print(f"📁 Loaded {len(history)} previously notified jobs\n")
    
    # Initialize sources (13 total - APIs + India sources)
    sources = [
        # === Direct API Sources (no login required) ===
        RemoteOKSource(),           # Remote tech jobs
        ArbeitnowSource(),          # EU/worldwide
        FindworkSource(),           # Dev jobs
        HimalayasSource(),          # Remote jobs
        JobicySource(),             # Remote data jobs
        TheMuseSource(),            # Various jobs
        WeWorkRemotelySource(),     # RSS feed - remote jobs
        
        # === India-Focused Sources (fresh jobs) ===
        IndeedIndiaSource(),        # Indeed India - Playwright (fresh!)
    ]
    
    # Add SerpAPI sources only during designated hours (to conserve 250/month quota)
    current_hour = datetime.now().hour
    if current_hour in SERPAPI_RUN_HOURS:
        sources.extend([
            GoogleJobsSource(),         # Google Jobs via SerpAPI (FRESH - last 24h!)
            NaukriGoogleSource(),       # Naukri via Google SerpAPI (indexed)
        ])
        serpapi_active = True
    else:
        serpapi_active = False
    
    # Add remaining sources
    sources.extend([
        # === Additional Aggregators ===
        LandingJobsSource(),        # EU jobs with relocation
        HNHiringSource(),           # Hacker News monthly hiring
        DuckDuckGoJobSource(),      # Meta-search engine
    ])
    
    # Show SerpAPI usage
    serpapi_usage = get_serpapi_usage()
    print(f"🌐 Fetching from {len(sources)} job sources (priority: fresh jobs)")
    if serpapi_active:
        print(f"📊 SerpAPI: ACTIVE (hour {current_hour}) - {serpapi_usage}/{SERPAPI_MONTHLY_LIMIT} calls used")
    else:
        print(f"📊 SerpAPI: SKIPPED (runs at hours {SERPAPI_RUN_HOURS}) - {serpapi_usage}/{SERPAPI_MONTHLY_LIMIT} calls used")
    
    all_jobs = []
    stats = {"scanned": 0, "new": 0, "matches": 0, "alerts": 0, "best": 0}
    
    # Fetch from all sources
    for config in SEARCH_CONFIGS:
        print(f"\n{'─' * 60}")
        print(f"🔍 {config['tag']}: {', '.join(config['keywords'][:3])}...")
        
        for source in sources:
            try:
                jobs = source.fetch_jobs(config['keywords'])
                for job in jobs:
                    job['tag'] = config['tag']
                    job['profile'] = config['profile']
                all_jobs.extend(jobs)
            except Exception as e:
                print(f"  ⚠️ {source.name}: {str(e)[:60]}")
            
            rate_limit(1.5)  # Longer delay to avoid rate limiting
    
    # Process jobs
    print(f"\n{'─' * 60}")
    print(f"📊 Processing {len(all_jobs)} total jobs...")
    
    # Apply freshness filter
    fresh_jobs = [job for job in all_jobs if is_job_fresh(job)]
    filtered_count = len(all_jobs) - len(fresh_jobs)
    print(f"   Fresh jobs (≤{MAX_JOB_AGE_HOURS}h): {len(fresh_jobs)} (filtered out: {filtered_count})")
    
    new_matches = []
    stats['filtered'] = filtered_count
    
    for job in fresh_jobs:
        job_id = generate_job_id(job)
        
        if job_id in history:
            continue
        
        stats['new'] += 1
        
        score = get_match_score(job['profile'], job)
        job['score'] = score
        job['id'] = job_id
        
        if score > stats['best']:
            stats['best'] = score
        
        if score >= MIN_MATCH_SCORE:
            new_matches.append(job)
            stats['matches'] += 1
    
    # Sort by score
    new_matches.sort(key=lambda x: x['score'], reverse=True)
    
    print(f"   New jobs: {stats['new']}")
    print(f"   Matches (>= {MIN_MATCH_SCORE}%): {stats['matches']}")
    print(f"   Best score: {stats['best']}%")
    
    # Send notifications
    print(f"\n{'─' * 60}")
    
    if not new_matches:
        print("📭 No new matching jobs found")
    else:
        print(f"📬 Sending {min(len(new_matches), 15)} notifications...\n")
        
        for job in new_matches[:15]:
            msg = (
                f"🎯 <b>{job['tag']} - {job['score']}% Match</b>\n\n"
                f"💼 <b>{job['title']}</b>\n"
                f"🏢 {job.get('company', 'Unknown')}\n"
                f"📍 {job.get('location', 'Unknown')}\n"
                f"🌐 {job['source']}\n\n"
                f"<a href=\"{job['url']}\">🔗 Apply Now</a>"
            )
            
            if telegram_ok:
                if send_telegram(msg):
                    print(f"  ✅ {job['title'][:45]}... ({job['score']}%)")
                    stats['alerts'] += 1
                else:
                    print(f"  ❌ Failed: {job['title'][:45]}...")
            else:
                print(f"  📝 [No Telegram] {job['title'][:45]}... ({job['score']}%)")
            
            history.add(job['id'])
            rate_limit()
    
    # Save history
    save_history(history)
    
    # Summary
    print(f"\n{'=' * 70}")
    print("🏁 SCAN COMPLETE")
    print(f"{'=' * 70}")
    print(f"   Total jobs fetched: {len(all_jobs)}")
    print(f"   Fresh jobs (≤{MAX_JOB_AGE_HOURS}h): {len(all_jobs) - stats.get('filtered', 0)}")
    print(f"   New jobs found: {stats['new']}")
    print(f"   Matches sent: {stats['alerts']}")
    print(f"   Best match: {stats['best']}%")
    print(f"   History size: {len(history)}")
    print(f"{'=' * 70}")
    
    # Send summary to Telegram
    if telegram_ok and stats['new'] > 0:
        fresh_count = len(all_jobs) - stats.get('filtered', 0)
        summary = (
            f"🤖 <b>Job Scan Complete</b>\n\n"
            f"📊 Fetched: {len(all_jobs)} jobs\n"
            f"⏱️ Fresh (≤{MAX_JOB_AGE_HOURS}h): {fresh_count}\n"
            f"🆕 New: {stats['new']}\n"
            f"📬 Alerts: {stats['alerts']}\n"
            f"🏆 Best: {stats['best']}%"
        )
        send_telegram(summary)


if __name__ == "__main__":
    main()
