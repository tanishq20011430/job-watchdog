"""
Notification services (Telegram, console, etc.)
"""

import asyncio
from typing import List, Optional
from datetime import datetime
import logging

import httpx

from ..database.models import ProcessedJob, NotificationPayload
from ..config.settings import settings

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Send job alerts to Telegram"""
    
    def __init__(self):
        self.token = settings.telegram.token
        self.chat_id = settings.telegram.chat_id
        self.base_url = f"https://api.telegram.org/bot{self.token}" if self.token else None
    
    @property
    def is_configured(self) -> bool:
        return settings.telegram.is_configured
    
    async def test_connection(self) -> bool:
        """Test Telegram bot connection"""
        if not self.is_configured:
            logger.warning("Telegram not configured - set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID")
            return False
        
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(f"{self.base_url}/getMe")
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('ok'):
                        bot_name = data['result'].get('username', 'Unknown')
                        logger.info(f"Telegram connected: @{bot_name}")
                        return True
                
                logger.error(f"Telegram verification failed: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Telegram connection error: {e}")
            return False
    
    def _format_job_message(self, job: ProcessedJob) -> str:
        """Format job for Telegram message"""
        # Score emoji
        if job.combined_score >= 0.7:
            score_emoji = "🔥"
        elif job.combined_score >= 0.5:
            score_emoji = "✨"
        else:
            score_emoji = "📌"
        
        # Category tag
        category_tags = {
            "Data Science": "🧪 DS",
            "Data Analytics": "📊 DA",
            "ML Engineering": "🤖 ML",
            "BI Developer": "📈 BI",
            "Data Engineering": "⚙️ DE",
        }
        category = category_tags.get(job.category.value, "💼")
        
        # Location
        location = job.city or job.location or "India"
        if job.is_remote:
            location = f"🏠 Remote / {location}"
        
        # Build message
        score_pct = int(job.combined_score * 100)
        
        message = (
            f"{score_emoji} <b>{category} - {score_pct}% Match</b>\n\n"
            f"💼 <b>{job.title}</b>\n"
            f"🏢 {job.company}\n"
            f"📍 {location}\n"
            f"🌐 {job.source}\n"
        )
        
        if job.posted:
            message += f"⏰ {job.posted}\n"
        
        if job.salary:
            message += f"💰 {job.salary}\n"
        
        if job.llm_experience_required:
            message += f"📋 Exp: {job.llm_experience_required}\n"
        
        message += f"\n<a href=\"{job.url}\">🔗 Apply Now</a>"
        
        return message
    
    async def send_job(self, job: ProcessedJob) -> bool:
        """Send single job notification"""
        if not self.is_configured:
            return False
        
        message = self._format_job_message(job)
        
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.post(
                    f"{self.base_url}/sendMessage",
                    data={
                        "chat_id": self.chat_id,
                        "text": message,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": "false"
                    }
                )
                
                success = response.status_code == 200
                if not success:
                    logger.warning(f"Telegram send failed: {response.status_code}")
                return success
        except Exception as e:
            logger.error(f"Telegram send error: {e}")
            return False
    
    async def send_jobs_batch(self, jobs: List[ProcessedJob], delay: float = 1.0) -> int:
        """Send multiple job notifications with rate limiting"""
        if not self.is_configured:
            return 0
        
        sent_count = 0
        for job in jobs:
            if await self.send_job(job):
                sent_count += 1
                logger.debug(f"Sent: {job.title[:40]}...")
            await asyncio.sleep(delay)
        
        return sent_count
    
    async def send_summary(self, total_fetched: int, total_new: int, 
                          total_matched: int, total_notified: int, 
                          best_score: float) -> bool:
        """Send scan summary"""
        if not self.is_configured:
            return False
        
        message = (
            f"🤖 <b>Job Scan Complete</b>\n\n"
            f"📊 Fetched: {total_fetched} jobs\n"
            f"🆕 New: {total_new}\n"
            f"✅ Matched: {total_matched}\n"
            f"📬 Notified: {total_notified}\n"
            f"🏆 Best: {int(best_score * 100)}%\n\n"
            f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.post(
                    f"{self.base_url}/sendMessage",
                    data={
                        "chat_id": self.chat_id,
                        "text": message,
                        "parse_mode": "HTML"
                    }
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Telegram summary error: {e}")
            return False


class ConsoleNotifier:
    """Console output for jobs (useful for testing)"""
    
    def format_job(self, job: ProcessedJob) -> str:
        """Format job for console output"""
        score_pct = int(job.combined_score * 100)
        return (
            f"\n{'=' * 60}\n"
            f"[{score_pct}%] {job.title}\n"
            f"Company: {job.company}\n"
            f"Location: {job.location} {'(Remote)' if job.is_remote else ''}\n"
            f"Source: {job.source}\n"
            f"Category: {job.category.value}\n"
            f"URL: {job.url}\n"
            f"{'=' * 60}"
        )
    
    def print_job(self, job: ProcessedJob):
        """Print job to console"""
        print(self.format_job(job))
    
    def print_jobs(self, jobs: List[ProcessedJob]):
        """Print multiple jobs"""
        print(f"\n{'🎯' * 10} TOP MATCHES {'🎯' * 10}\n")
        for job in jobs:
            self.print_job(job)


# Global instances
_telegram = None
_console = None


def get_telegram_notifier() -> TelegramNotifier:
    """Get Telegram notifier instance"""
    global _telegram
    if _telegram is None:
        _telegram = TelegramNotifier()
    return _telegram


def get_console_notifier() -> ConsoleNotifier:
    """Get console notifier instance"""
    global _console
    if _console is None:
        _console = ConsoleNotifier()
    return _console
