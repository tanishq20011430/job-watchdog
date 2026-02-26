"""
Main Job Watchdog Orchestrator.
Coordinates all components: sources, matching, filtering, notifications.
"""

import asyncio
import sys
from datetime import datetime
from typing import List, Optional
import logging

from .config.settings import settings
from .database.models import RawJob, ProcessedJob, JobBatch, ScanStats, JobStatus
from .database.repository import db
from .sources.base import (
    BaseJobSource, RemoteOKSource, ArbeitnowSource, 
    HimalayasSource, JobicySource, FindworkSource,
    TheMuseSource, HNHiringSource
)
from .sources.india import (
    NaukriSource, FounditSource, InstahyreSource,
    CutshortSource, HiristSource, LinkedInIndiaSource,
    GoogleJobsSource, GoogleJobsDirectSource, AdzunaIndiaSource, IndeedIndiaPlaywrightSource
)
from .sources.free_apis import (
    GreenhouseMultiSource, LeverMultiSource, WorkingNomadsSource,
    RemotiveSource, WeWorkRemotelySource, YCJobsSource, StartupJobsSource,
    AshbyMultiSource, EuropeRemoteSource, CryptoJobsSource, IndiaStartupsSource
)
from .matching.semantic import get_matcher
from .filters.llm_filter import get_llm_filter, get_quick_filter
from .utils.notifications import get_telegram_notifier, get_console_notifier

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(settings.log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


# Search keywords for different profiles
SEARCH_KEYWORDS = {
    "data_science": [
        "data scientist", "machine learning", "ml engineer", 
        "deep learning", "nlp engineer", "ai engineer",
        "data science", "applied scientist", "research scientist",
        "computer vision", "llm engineer", "ai ml"
    ],
    "data_analytics": [
        "data analyst", "business analyst", "analytics",
        "sql analyst", "reporting analyst", "bi analyst",
        "product analyst", "analytics engineer", "insight analyst"
    ],
    "bi_developer": [
        "power bi", "tableau", "bi developer", 
        "business intelligence", "visualization analyst",
        "dashboard developer", "looker", "qlik"
    ],
    "data_engineering": [
        "data engineer", "etl developer", "python developer",
        "spark developer", "airflow", "dbt developer",
        "big data engineer", "analytics engineer"
    ],
    "general_tech": [
        "python", "sql", "machine learning engineer",
        "artificial intelligence", "data", "analyst"
    ]
}


class JobWatchdog:
    """
    Main orchestrator for the job watchdog system.
    Manages sources, matching, filtering, and notifications.
    """
    
    def __init__(self):
        self.stats = ScanStats()
        self.matcher = get_matcher()
        self.llm_filter = get_llm_filter()
        self.quick_filter = get_quick_filter()
        self.telegram = get_telegram_notifier()
        self.console = get_console_notifier()
        
        # Initialize sources
        self.sources: List[BaseJobSource] = self._init_sources()
    
    def _init_sources(self) -> List[BaseJobSource]:
        """Initialize all job sources"""
        sources = []
        
        # === India-Focused Sources (Primary) ===
        sources.extend([
            NaukriSource(),
            # FounditSource(),  # Uncomment when API is verified
            # InstahyreSource(),  # Uncomment when API is verified
            # CutshortSource(),
            # HiristSource(),
            LinkedInIndiaSource(),
        ])
        
        # === Playwright-based (requires playwright) ===
        # Uncomment if playwright is installed
        # sources.append(IndeedIndiaPlaywrightSource())
        
        # === Global Remote Sources (filter to India) ===
        sources.extend([
            RemoteOKSource(),
            ArbeitnowSource(),
            HimalayasSource(),
            JobicySource(),
            FindworkSource(),
            TheMuseSource(),
            HNHiringSource(),
        ])
        
        # === Google Jobs via SerpAPI (best source for fresh India jobs) ===
        # Always enabled if API key is configured (uses smart quota management)
        if settings.serpapi.api_key:
            sources.append(GoogleJobsSource())
            logger.info("Google Jobs enabled (SerpAPI configured)")
        
        # === Adzuna - FREE API (1000 calls/month) ===
        # Sign up at https://developer.adzuna.com/
        import os
        if os.getenv('ADZUNA_APP_ID') and os.getenv('ADZUNA_APP_KEY'):
            sources.append(AdzunaIndiaSource())
            logger.info("Adzuna enabled (FREE API)")
        
        # === FREE APIs (No keys needed!) ===
        sources.extend([
            GreenhouseMultiSource(),    # 100+ top tech companies
            LeverMultiSource(),          # 60+ tech companies
            WorkingNomadsSource(),       # Remote jobs
            RemotiveSource(),            # Remote-first jobs
            WeWorkRemotelySource(),      # Large remote board RSS
            YCJobsSource(),              # YC startup jobs
            StartupJobsSource(),         # Startup jobs
            AshbyMultiSource(),          # YC startups using Ashby ATS
            EuropeRemoteSource(),        # JustJoin.it Europe remote
            CryptoJobsSource(),          # Web3/Crypto jobs
            IndiaStartupsSource(),       # India unicorn startups
        ])
        logger.info(f"Added 11 FREE API sources (no keys needed)")
        
        return sources
    
    def _get_all_keywords(self) -> List[str]:
        """Get combined list of all search keywords"""
        all_kw = []
        for keywords in SEARCH_KEYWORDS.values():
            all_kw.extend(keywords)
        return list(set(all_kw))
    
    async def _fetch_from_source(self, source: BaseJobSource, keywords: List[str]) -> JobBatch:
        """Fetch jobs from a single source with error handling"""
        try:
            logger.debug(f"Fetching from {source.name}...")
            batch = await source.fetch_jobs(keywords)
            
            if batch.error:
                logger.warning(f"{source.name}: {batch.error}")
                self.stats.errors.append(f"{source.name}: {batch.error}")
            else:
                logger.info(f"{source.name}: {batch.count} jobs in {batch.fetch_duration_ms}ms")
            
            return batch
        except Exception as e:
            error_msg = f"{source.name}: {str(e)[:100]}"
            logger.error(error_msg)
            self.stats.errors.append(error_msg)
            return JobBatch(source=source.name, error=str(e))
        finally:
            await source.close()
    
    async def fetch_all_jobs(self) -> List[RawJob]:
        """Fetch jobs from all sources concurrently"""
        keywords = self._get_all_keywords()
        
        logger.info(f"Fetching from {len(self.sources)} sources...")
        logger.info(f"Keywords: {', '.join(keywords[:5])}...")
        
        # Create tasks for concurrent fetching
        tasks = [
            self._fetch_from_source(source, keywords)
            for source in self.sources
        ]
        
        # Fetch with semaphore to limit concurrency
        semaphore = asyncio.Semaphore(settings.search.max_concurrent_requests)
        
        async def fetch_with_limit(task):
            async with semaphore:
                return await task
        
        limited_tasks = [fetch_with_limit(task) for task in tasks]
        batches = await asyncio.gather(*limited_tasks, return_exceptions=True)
        
        # Combine all jobs
        all_jobs: List[RawJob] = []
        for batch in batches:
            if isinstance(batch, Exception):
                logger.error(f"Fetch exception: {batch}")
                continue
            if isinstance(batch, JobBatch):
                all_jobs.extend(batch.jobs)
                self.stats.source_counts[batch.source] = batch.count
        
        self.stats.total_fetched = len(all_jobs)
        logger.info(f"Total fetched: {len(all_jobs)} jobs")
        
        return all_jobs
    
    def deduplicate_jobs(self, jobs: List[RawJob]) -> List[RawJob]:
        """Remove duplicate jobs based on job_id"""
        # Get known job IDs from database
        known_ids = db.get_known_job_ids()
        
        seen_ids = set()
        unique_jobs = []
        
        for job in jobs:
            if job.job_id in known_ids:
                continue  # Already in database
            if job.job_id in seen_ids:
                continue  # Duplicate in current batch
            
            seen_ids.add(job.job_id)
            unique_jobs.append(job)
        
        self.stats.total_new = len(unique_jobs)
        logger.info(f"New jobs: {len(unique_jobs)} (filtered {len(jobs) - len(unique_jobs)} duplicates)")
        
        return unique_jobs
    
    def process_jobs(self, jobs: List[RawJob]) -> List[ProcessedJob]:
        """Process and score all jobs"""
        logger.info(f"Processing {len(jobs)} jobs with semantic matcher...")
        
        processed = self.matcher.match_jobs(jobs)
        
        # Count by status
        relevant = [j for j in processed if j.status == JobStatus.DETECTED]
        filtered_location = [j for j in processed if not j.is_india]
        filtered_age = [j for j in processed if j.is_india and j.age_hours > settings.search.max_job_age_hours]
        filtered_score = [j for j in processed if j.is_india and j.age_hours <= settings.search.max_job_age_hours and j.status == JobStatus.FILTERED]
        
        self.stats.total_filtered_location = len(filtered_location)
        self.stats.total_filtered_relevance = len(filtered_score) + len(filtered_age)
        
        logger.info(f"Relevant: {len(relevant)}, Filtered (location): {len(filtered_location)}, Filtered (age >{settings.search.max_job_age_hours}h): {len(filtered_age)}, Filtered (score): {len(filtered_score)}")
        
        return processed
    
    async def apply_llm_filter(self, jobs: List[ProcessedJob]) -> List[ProcessedJob]:
        """Apply LLM filtering to relevant jobs"""
        relevant = [j for j in jobs if j.status == JobStatus.DETECTED]
        
        if not relevant:
            return jobs
        
        if not settings.llm.enabled:
            logger.info("LLM filtering disabled")
            return jobs
        
        # First apply quick regex filter
        logger.info(f"Applying quick experience filter to {len(relevant)} jobs...")
        
        for job in relevant:
            suitable, reason = self.quick_filter.check_experience(job.title, job.description)
            if not suitable:
                job.status = JobStatus.FILTERED
                job.llm_reason = reason
                self.stats.total_filtered_llm += 1
        
        # Then apply LLM filter to remaining
        remaining = [j for j in relevant if j.status == JobStatus.DETECTED]
        
        if remaining and settings.llm.enabled and (settings.llm.groq_api_key or settings.llm.ollama_url):
            logger.info(f"Applying LLM filter to {len(remaining)} jobs...")
            
            try:
                results = await self.llm_filter.filter_jobs_batch(remaining, concurrency=3)
                
                # Apply results
                results_map = {r.job_id: r for r in results}
                for job in remaining:
                    if job.job_id in results_map:
                        result = results_map[job.job_id]
                        job.llm_suitable = result.suitable
                        job.llm_experience_required = result.experience_required
                        job.llm_reason = result.reason
                        
                        if not result.suitable:
                            job.status = JobStatus.FILTERED
                            self.stats.total_filtered_llm += 1
            except Exception as e:
                logger.warning(f"LLM filter error: {e}")
        
        return jobs
    
    def get_top_matches(self, jobs: List[ProcessedJob], limit: int = 20) -> List[ProcessedJob]:
        """Get top matching jobs for notification"""
        relevant = [
            j for j in jobs 
            if j.status == JobStatus.DETECTED 
            and j.is_india
            and j.combined_score >= settings.matching.min_semantic_score
        ]
        
        # Sort by score
        relevant.sort(key=lambda x: x.combined_score, reverse=True)
        
        self.stats.total_matched = len(relevant)
        if relevant:
            self.stats.best_score = relevant[0].combined_score
            scores = [j.combined_score for j in relevant]
            self.stats.avg_score = sum(scores) / len(scores)
        
        logger.info(f"Top matches: {len(relevant)}, Best score: {self.stats.best_score:.2%}")
        
        return relevant[:limit]
    
    async def send_notifications(self, jobs: List[ProcessedJob]) -> int:
        """Send notifications for matched jobs (skip already-notified)"""
        if not jobs:
            logger.info("No jobs to notify")
            return 0
        
        # Filter out already-notified jobs from database
        already_notified = db.get_notified_job_ids()
        jobs_to_notify = [j for j in jobs if j.job_id not in already_notified]
        
        if not jobs_to_notify:
            logger.info(f"All {len(jobs)} jobs already notified - skipping")
            return 0
        
        skipped = len(jobs) - len(jobs_to_notify)
        if skipped > 0:
            logger.info(f"Skipping {skipped} already-notified jobs")
        
        # Console output
        self.console.print_jobs(jobs_to_notify[:10])
        
        # Telegram notifications
        if self.telegram.is_configured:
            logger.info(f"Sending {len(jobs_to_notify)} Telegram notifications...")
            sent = await self.telegram.send_jobs_batch(jobs_to_notify)
            self.stats.total_notified = sent
            
            # Update job status in memory AND database
            for job in jobs_to_notify[:sent]:
                job.status = JobStatus.NOTIFIED
                job.notified_at = datetime.now()
                # Persist to database
                db.update_job_status(job.job_id, JobStatus.NOTIFIED)
            
            logger.info(f"Sent {sent} notifications (marked as notified in DB)")
            return sent
        else:
            logger.warning("Telegram not configured - notifications skipped")
            return 0
    
    async def run(self) -> ScanStats:
        """
        Main execution flow.
        Fetches, processes, filters, and notifies.
        """
        print("\n" + "=" * 70)
        print("🐕 JOB WATCHDOG - Intelligent Job Scanner")
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70 + "\n")
        
        # Test Telegram
        if self.telegram.is_configured:
            await self.telegram.test_connection()
        else:
            print("⚠️  Telegram not configured - set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in .env")
        
        # Database stats
        db_stats = db.get_stats()
        print(f"📊 Database: {db_stats['total_jobs']} total jobs")
        
        # Phase 1: Fetch
        print("\n" + "-" * 50)
        print("📥 PHASE 1: Fetching Jobs")
        print("-" * 50)
        
        raw_jobs = await self.fetch_all_jobs()
        
        # Phase 2: Deduplicate
        print("\n" + "-" * 50)
        print("🔍 PHASE 2: Deduplication")
        print("-" * 50)
        
        new_jobs = self.deduplicate_jobs(raw_jobs)
        
        if not new_jobs:
            print("📭 No new jobs found")
            self.stats.completed_at = datetime.now()
            return self.stats
        
        # Phase 3: Semantic Matching
        print("\n" + "-" * 50)
        print("🧠 PHASE 3: Semantic Matching")
        print("-" * 50)
        
        processed_jobs = self.process_jobs(new_jobs)
        
        # Phase 4: LLM Filtering
        print("\n" + "-" * 50)
        print("🤖 PHASE 4: Experience Filtering")
        print("-" * 50)
        
        processed_jobs = await self.apply_llm_filter(processed_jobs)
        
        # Phase 5: Get Top Matches
        print("\n" + "-" * 50)
        print("🎯 PHASE 5: Top Matches")
        print("-" * 50)
        
        top_matches = self.get_top_matches(processed_jobs, limit=15)
        
        # Phase 6: Save to Database
        print("\n" + "-" * 50)
        print("💾 PHASE 6: Saving to Database")
        print("-" * 50)
        
        inserted = db.insert_jobs_batch(processed_jobs)
        print(f"Saved {inserted} jobs to database")
        
        # Phase 7: Notifications
        print("\n" + "-" * 50)
        print("📬 PHASE 7: Notifications")
        print("-" * 50)
        
        sent = await self.send_notifications(top_matches)
        
        # Summary
        self.stats.completed_at = datetime.now()
        
        print("\n" + "=" * 70)
        print("🏁 SCAN COMPLETE")
        print("=" * 70)
        print(f"   Duration: {self.stats.duration_seconds:.1f}s")
        print(f"   Fetched: {self.stats.total_fetched}")
        print(f"   New: {self.stats.total_new}")
        print(f"   Matched: {self.stats.total_matched}")
        print(f"   Notified: {self.stats.total_notified}")
        print(f"   Filtered (location): {self.stats.total_filtered_location}")
        print(f"   Filtered (relevance): {self.stats.total_filtered_relevance}")
        print(f"   Filtered (experience): {self.stats.total_filtered_llm}")
        print(f"   Best Score: {self.stats.best_score:.1%}")
        print("=" * 70)
        
        # Save stats
        db.save_scan_stats(self.stats)
        
        # Send summary to Telegram
        if self.telegram.is_configured and self.stats.total_notified > 0:
            await self.telegram.send_summary(
                self.stats.total_fetched,
                self.stats.total_new,
                self.stats.total_matched,
                self.stats.total_notified,
                self.stats.best_score
            )
        
        return self.stats


async def main():
    """Main entry point"""
    watchdog = JobWatchdog()
    await watchdog.run()


def run():
    """Synchronous entry point"""
    asyncio.run(main())


if __name__ == "__main__":
    run()
