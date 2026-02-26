"""
LLM-powered job filtering.
Uses Groq (free tier), Ollama (local), or OpenAI.
Extracts experience requirements and determines suitability.
"""

import asyncio
import json
import re
from typing import Optional, List
import logging

from ..database.models import ProcessedJob, LLMFilterResult, JobStatus
from ..config.settings import settings

logger = logging.getLogger(__name__)


class LLMFilter:
    """
    LLM-based job filtering.
    Extracts experience requirements and determines job suitability.
    """
    
    SYSTEM_PROMPT = """You are a job requirements analyzer. Given a job posting, extract:
1. Required years of experience (number or range)
2. Whether the role is suitable for someone with 0-3 years of experience

Be precise and only use information from the job description. If experience is not mentioned, assume entry-level friendly.

Respond in JSON format only:
{
    "experience_required": "0-2 years" or "5+ years" or "Not specified",
    "suitable_for_junior": true or false,
    "reason": "Brief explanation"
}"""

    USER_PROMPT_TEMPLATE = """Analyze this job posting:

Title: {title}
Company: {company}

Description:
{description}

Is this job suitable for a candidate with 0-3 years of experience in Data Science/Analytics?"""

    def __init__(self):
        self.provider = settings.llm.provider
        self.enabled = settings.llm.enabled
        self._client = None
    
    async def _call_groq(self, prompt: str) -> Optional[str]:
        """Call Groq API (free tier: llama-3.1-8b-instant)"""
        if not settings.llm.groq_api_key:
            logger.warning("GROQ_API_KEY not set")
            return None
        
        import httpx
        
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.llm.groq_api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "llama-3.1-8b-instant",  # Free tier model
                    "messages": [
                        {"role": "system", "content": self.SYSTEM_PROMPT},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.1,
                    "max_tokens": 200,
                    "response_format": {"type": "json_object"}
                }
            )
            
            if response.status_code != 200:
                logger.warning(f"Groq API error: {response.status_code}")
                return None
            
            data = response.json()
            return data["choices"][0]["message"]["content"]
    
    async def _call_ollama(self, prompt: str) -> Optional[str]:
        """Call local Ollama"""
        import httpx
        
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.post(
                    f"{settings.llm.ollama_url}/api/generate",
                    json={
                        "model": settings.llm.ollama_model,
                        "prompt": f"{self.SYSTEM_PROMPT}\n\n{prompt}",
                        "stream": False,
                        "format": "json"
                    }
                )
                
                if response.status_code != 200:
                    logger.warning(f"Ollama error: {response.status_code}")
                    return None
                
                data = response.json()
                return data.get("response", "")
        except Exception as e:
            logger.warning(f"Ollama connection error: {e}")
            return None
    
    async def _call_openai(self, prompt: str) -> Optional[str]:
        """Call OpenAI API"""
        if not settings.llm.openai_api_key:
            logger.warning("OPENAI_API_KEY not set")
            return None
        
        import httpx
        
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.llm.openai_api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "gpt-3.5-turbo",
                    "messages": [
                        {"role": "system", "content": self.SYSTEM_PROMPT},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.1,
                    "max_tokens": 200,
                    "response_format": {"type": "json_object"}
                }
            )
            
            if response.status_code != 200:
                logger.warning(f"OpenAI API error: {response.status_code}")
                return None
            
            data = response.json()
            return data["choices"][0]["message"]["content"]
    
    def _parse_response(self, response: str) -> Optional[dict]:
        """Parse LLM response JSON"""
        try:
            # Clean response
            response = response.strip()
            
            # Try to extract JSON from response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            
            return json.loads(response)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse LLM response: {e}")
            return None
    
    async def filter_job(self, job: ProcessedJob) -> LLMFilterResult:
        """
        Analyze a job with LLM and determine suitability.
        """
        if not self.enabled:
            return LLMFilterResult(
                job_id=job.job_id,
                suitable=True,
                experience_required=None,
                reason="LLM filtering disabled",
                confidence=0.0
            )
        
        # Build prompt
        prompt = self.USER_PROMPT_TEMPLATE.format(
            title=job.title,
            company=job.company,
            description=job.description[:2000]  # Limit for context
        )
        
        # Call appropriate LLM
        response = None
        if self.provider == "groq":
            response = await self._call_groq(prompt)
        elif self.provider == "ollama":
            response = await self._call_ollama(prompt)
        elif self.provider == "openai":
            response = await self._call_openai(prompt)
        
        if not response:
            # Default to suitable if LLM fails
            return LLMFilterResult(
                job_id=job.job_id,
                suitable=True,
                experience_required=None,
                reason="LLM call failed, defaulting to suitable",
                confidence=0.0
            )
        
        # Parse response
        parsed = self._parse_response(response)
        
        if not parsed:
            return LLMFilterResult(
                job_id=job.job_id,
                suitable=True,
                experience_required=None,
                reason="Failed to parse LLM response",
                confidence=0.0
            )
        
        suitable = parsed.get("suitable_for_junior", True)
        experience = parsed.get("experience_required", "Not specified")
        reason = parsed.get("reason", "")
        
        return LLMFilterResult(
            job_id=job.job_id,
            suitable=suitable,
            experience_required=experience,
            reason=reason,
            confidence=0.8  # LLM confidence
        )
    
    async def filter_jobs_batch(self, jobs: List[ProcessedJob], concurrency: int = 5) -> List[LLMFilterResult]:
        """
        Filter multiple jobs in parallel with rate limiting.
        Only filters jobs that passed initial semantic matching.
        """
        if not self.enabled:
            return [
                LLMFilterResult(job_id=j.job_id, suitable=True, reason="LLM disabled")
                for j in jobs
            ]
        
        # Only filter relevant jobs (score >= threshold)
        relevant_jobs = [j for j in jobs if j.combined_score >= settings.matching.min_semantic_score]
        
        logger.info(f"LLM filtering {len(relevant_jobs)} jobs...")
        
        semaphore = asyncio.Semaphore(concurrency)
        
        async def filter_with_semaphore(job):
            async with semaphore:
                result = await self.filter_job(job)
                await asyncio.sleep(0.5)  # Rate limiting
                return result
        
        tasks = [filter_with_semaphore(job) for job in relevant_jobs]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        filtered_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"LLM filter error for job: {result}")
                filtered_results.append(LLMFilterResult(
                    job_id=relevant_jobs[i].job_id,
                    suitable=True,
                    reason=f"Error: {str(result)[:50]}"
                ))
            else:
                filtered_results.append(result)
        
        return filtered_results


class QuickExperienceFilter:
    """
    Fast regex-based experience filter (no LLM required).
    Use as pre-filter before LLM to save API calls.
    
    NOTE: Only filter on CLEAR senior patterns. Many "lead" or "staff" 
    roles are accessible. Be lenient to avoid false negatives.
    """
    
    # Patterns that indicate CLEARLY senior roles (strict - title only)
    SENIOR_PATTERNS = [
        r'\b(principal|staff\s+engineer|architect)\b',
        r'\b(director|vp|vice president|head of)\b',
        r'\b(10\+|12\+|15\+|20\+)\s*(?:years?|yrs?)\b',
    ]
    
    # Patterns that indicate junior-friendly
    JUNIOR_PATTERNS = [
        r'\b(junior|jr\.?|entry.?level|fresher|graduate)\b',
        r'\b(0-[0-5]|[0-3]\s*(?:\+|to|-)\s*[0-5])\s*(?:years?|yrs?)\b',
        r'\bno.?experience.?required\b',
        r'\bfreshers?.?welcome\b',
        r'\bearly.?career\b',
    ]
    
    def check_experience(self, title: str, description: str) -> tuple[bool, str]:
        """
        Quick check for experience requirements.
        Returns: (is_suitable, reason)
        
        Strategy: Be lenient - only filter CLEAR senior roles.
        """
        title_lower = title.lower()
        text = f"{title} {description}".lower()
        
        # Check for CLEAR senior patterns in TITLE only
        for pattern in self.SENIOR_PATTERNS:
            if re.search(pattern, title_lower, re.IGNORECASE):
                match = re.search(pattern, title_lower, re.IGNORECASE)
                return False, f"Senior role: '{match.group()}'"
        
        # Check for junior patterns (qualifying)
        for pattern in self.JUNIOR_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                match = re.search(pattern, text, re.IGNORECASE)
                return True, f"Junior-friendly: '{match.group()}'"
        
        # Extract experience numbers - only filter if CLEARLY requires 8+ years
        exp_pattern = r'(\d+)\s*(?:\+)?\s*(?:years?|yrs?)(?:\s+(?:of\s+)?(?:experience|exp))?'
        matches = re.findall(exp_pattern, text, re.IGNORECASE)
        
        for match in matches:
            min_exp = int(match)
            # Only filter if minimum experience exceeds 7 years
            if min_exp >= 8:
                return False, f"Requires {min_exp}+ years experience"
        
        # Default: assume suitable if no clear disqualifying patterns
        return True, "No clear senior requirement found"


# Global instances
_llm_filter = None
_quick_filter = None


def get_llm_filter() -> LLMFilter:
    """Get or create LLM filter instance"""
    global _llm_filter
    if _llm_filter is None:
        _llm_filter = LLMFilter()
    return _llm_filter


def get_quick_filter() -> QuickExperienceFilter:
    """Get or create quick filter instance"""
    global _quick_filter
    if _quick_filter is None:
        _quick_filter = QuickExperienceFilter()
    return _quick_filter
