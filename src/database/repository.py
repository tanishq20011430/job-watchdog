"""
SQLite database layer for job state management.
Handles persistence, deduplication, and state tracking.
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Set
from contextlib import contextmanager

from ..database.models import ProcessedJob, JobStatus, ScanStats
from ..config.settings import settings


class JobDatabase:
    """SQLite database for job tracking"""
    
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or settings.database.db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    @contextmanager
    def _get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_db(self):
        """Initialize database schema"""
        with self._get_connection() as conn:
            conn.executescript("""
                -- Jobs table
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    company TEXT,
                    location TEXT,
                    description TEXT,
                    url TEXT,
                    source TEXT,
                    posted TEXT,
                    salary TEXT,
                    job_type TEXT,
                    
                    status TEXT DEFAULT 'detected',
                    category TEXT,
                    
                    semantic_score REAL DEFAULT 0,
                    keyword_score REAL DEFAULT 0,
                    combined_score REAL DEFAULT 0,
                    
                    llm_suitable INTEGER,
                    llm_experience_required TEXT,
                    llm_reason TEXT,
                    
                    is_india INTEGER DEFAULT 0,
                    is_remote INTEGER DEFAULT 0,
                    city TEXT,
                    age_hours REAL DEFAULT 0,
                    
                    fetched_at TIMESTAMP,
                    processed_at TIMESTAMP,
                    notified_at TIMESTAMP,
                    applied_at TIMESTAMP,
                    
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Indexes for common queries
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
                CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);
                CREATE INDEX IF NOT EXISTS idx_jobs_score ON jobs(combined_score DESC);
                CREATE INDEX IF NOT EXISTS idx_jobs_fetched ON jobs(fetched_at DESC);
                CREATE INDEX IF NOT EXISTS idx_jobs_is_india ON jobs(is_india);
                
                -- Scan history table
                CREATE TABLE IF NOT EXISTS scan_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    total_fetched INTEGER DEFAULT 0,
                    total_new INTEGER DEFAULT 0,
                    total_matched INTEGER DEFAULT 0,
                    total_notified INTEGER DEFAULT 0,
                    best_score REAL DEFAULT 0,
                    errors TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- SerpAPI usage tracking
                CREATE TABLE IF NOT EXISTS api_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_name TEXT NOT NULL,
                    month TEXT NOT NULL,
                    call_count INTEGER DEFAULT 0,
                    last_call_at TIMESTAMP,
                    UNIQUE(api_name, month)
                );
            """)
    
    def get_known_job_ids(self) -> Set[str]:
        """Get all known job IDs for deduplication"""
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT job_id FROM jobs")
            return {row['job_id'] for row in cursor.fetchall()}
    
    def get_notified_job_ids(self) -> Set[str]:
        """Get job IDs that were already notified (to prevent duplicates)"""
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT job_id FROM jobs WHERE status = 'notified'")
            return {row['job_id'] for row in cursor.fetchall()}
    
    def job_exists(self, job_id: str) -> bool:
        """Check if job already exists"""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM jobs WHERE job_id = ?", 
                (job_id,)
            )
            return cursor.fetchone() is not None
    
    def insert_job(self, job: ProcessedJob) -> bool:
        """Insert a new job, returns True if inserted"""
        if self.job_exists(job.job_id):
            return False
        
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO jobs (
                    job_id, title, company, location, description, url, source,
                    posted, salary, job_type, status, category,
                    semantic_score, keyword_score, combined_score,
                    llm_suitable, llm_experience_required, llm_reason,
                    is_india, is_remote, city, age_hours,
                    fetched_at, processed_at, notified_at, applied_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job.job_id, job.title, job.company, job.location, 
                job.description, job.url, job.source,
                job.posted, job.salary, job.job_type,
                job.status.value, job.category.value if job.category else None,
                job.semantic_score, job.keyword_score, job.combined_score,
                1 if job.llm_suitable else (0 if job.llm_suitable is False else None),
                job.llm_experience_required, job.llm_reason,
                1 if job.is_india else 0, 1 if job.is_remote else 0,
                job.city, job.age_hours,
                job.fetched_at, job.processed_at, job.notified_at, job.applied_at
            ))
        return True
    
    def insert_jobs_batch(self, jobs: List[ProcessedJob]) -> int:
        """Batch insert jobs, returns count of inserted"""
        known_ids = self.get_known_job_ids()
        new_jobs = [j for j in jobs if j.job_id not in known_ids]
        
        if not new_jobs:
            return 0
        
        with self._get_connection() as conn:
            for job in new_jobs:
                conn.execute("""
                    INSERT OR IGNORE INTO jobs (
                        job_id, title, company, location, description, url, source,
                        posted, salary, job_type, status, category,
                        semantic_score, keyword_score, combined_score,
                        llm_suitable, llm_experience_required, llm_reason,
                        is_india, is_remote, city, age_hours,
                        fetched_at, processed_at, notified_at, applied_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    job.job_id, job.title, job.company, job.location,
                    job.description, job.url, job.source,
                    job.posted, job.salary, job.job_type,
                    job.status.value, job.category.value if job.category else None,
                    job.semantic_score, job.keyword_score, job.combined_score,
                    1 if job.llm_suitable else (0 if job.llm_suitable is False else None),
                    job.llm_experience_required, job.llm_reason,
                    1 if job.is_india else 0, 1 if job.is_remote else 0,
                    job.city, job.age_hours,
                    job.fetched_at, job.processed_at, job.notified_at, job.applied_at
                ))
        return len(new_jobs)
    
    def update_job_status(self, job_id: str, status: JobStatus, **kwargs):
        """Update job status and optional fields"""
        updates = ["status = ?", "updated_at = CURRENT_TIMESTAMP"]
        values = [status.value]
        
        if status == JobStatus.NOTIFIED:
            updates.append("notified_at = ?")
            values.append(datetime.now())
        elif status == JobStatus.APPLIED:
            updates.append("applied_at = ?")
            values.append(datetime.now())
        
        for key, value in kwargs.items():
            updates.append(f"{key} = ?")
            values.append(value)
        
        values.append(job_id)
        
        with self._get_connection() as conn:
            conn.execute(
                f"UPDATE jobs SET {', '.join(updates)} WHERE job_id = ?",
                values
            )
    
    def get_jobs_by_status(self, status: JobStatus, limit: int = 100) -> List[dict]:
        """Get jobs by status"""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """SELECT * FROM jobs WHERE status = ? 
                   ORDER BY combined_score DESC LIMIT ?""",
                (status.value, limit)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_top_matches(self, limit: int = 20, min_score: float = 0.3) -> List[dict]:
        """Get top matching jobs"""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """SELECT * FROM jobs 
                   WHERE combined_score >= ? AND is_india = 1
                   ORDER BY combined_score DESC, fetched_at DESC
                   LIMIT ?""",
                (min_score, limit)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_recent_jobs(self, hours: int = 24, limit: int = 100) -> List[dict]:
        """Get recently fetched jobs"""
        cutoff = datetime.now() - timedelta(hours=hours)
        with self._get_connection() as conn:
            cursor = conn.execute(
                """SELECT * FROM jobs 
                   WHERE fetched_at >= ? 
                   ORDER BY fetched_at DESC LIMIT ?""",
                (cutoff, limit)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_stats(self) -> dict:
        """Get database statistics"""
        with self._get_connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            by_status = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM jobs GROUP BY status"
            ).fetchall()
            by_source = conn.execute(
                "SELECT source, COUNT(*) as cnt FROM jobs GROUP BY source ORDER BY cnt DESC"
            ).fetchall()
            avg_score = conn.execute(
                "SELECT AVG(combined_score) FROM jobs WHERE combined_score > 0"
            ).fetchone()[0]
            
            return {
                "total_jobs": total,
                "by_status": {row['status']: row['cnt'] for row in by_status},
                "by_source": {row['source']: row['cnt'] for row in by_source},
                "avg_score": round(avg_score or 0, 3)
            }
    
    def save_scan_stats(self, stats: ScanStats):
        """Save scan run statistics"""
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO scan_history (
                    started_at, completed_at, total_fetched, total_new,
                    total_matched, total_notified, best_score, errors
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                stats.started_at, stats.completed_at,
                stats.total_fetched, stats.total_new,
                stats.total_matched, stats.total_notified,
                stats.best_score, "\n".join(stats.errors)
            ))
    
    # API Usage tracking
    def get_api_usage(self, api_name: str) -> int:
        """Get current month's API usage"""
        month = datetime.now().strftime('%Y-%m')
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT call_count FROM api_usage WHERE api_name = ? AND month = ?",
                (api_name, month)
            )
            row = cursor.fetchone()
            return row['call_count'] if row else 0
    
    def increment_api_usage(self, api_name: str) -> int:
        """Increment API usage and return new count"""
        month = datetime.now().strftime('%Y-%m')
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO api_usage (api_name, month, call_count, last_call_at)
                VALUES (?, ?, 1, CURRENT_TIMESTAMP)
                ON CONFLICT(api_name, month) DO UPDATE SET
                    call_count = call_count + 1,
                    last_call_at = CURRENT_TIMESTAMP
            """, (api_name, month))
            
            cursor = conn.execute(
                "SELECT call_count FROM api_usage WHERE api_name = ? AND month = ?",
                (api_name, month)
            )
            return cursor.fetchone()['call_count']
    
    def cleanup_old_jobs(self, days: int = 30):
        """Remove jobs older than specified days"""
        cutoff = datetime.now() - timedelta(days=days)
        with self._get_connection() as conn:
            cursor = conn.execute(
                "DELETE FROM jobs WHERE fetched_at < ? AND status IN ('detected', 'filtered')",
                (cutoff,)
            )
            return cursor.rowcount


# Global database instance
db = JobDatabase()
