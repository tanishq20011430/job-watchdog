# 🐕 Job Watchdog v2.0

**Intelligent Multi-Source Job Alert System for India**

A complete redesign with semantic matching, LLM filtering, and async architecture for accurate, relevant job alerts.

## ✨ What's New in v2.0

| Feature | v1.0 | v2.0 |
|---------|------|------|
| Architecture | Single file, synchronous | Modular, async |
| Matching | TF-IDF + Keywords | Sentence Embeddings (Semantic) |
| Filtering | None | LLM-powered experience filter |
| Database | CSV file | SQLite with state tracking |
| Location | All regions | India-focused with city detection |
| Relevance | Low (sales jobs leaked through) | High (strict filtering) |

## 🎯 Key Features

- **🧠 Semantic Matching** - Uses sentence-transformers (all-MiniLM-L6-v2) to understand job descriptions, not just keywords
- **🤖 LLM Experience Filtering** - Uses Groq (free) or Ollama to filter out senior roles requiring 5+ years
- **🇮🇳 India-Focused** - Strict location filtering for Pune, Mumbai, Bangalore, Hyderabad, Delhi NCR
- **⚡ Async Architecture** - Fetches from all sources concurrently for 3x faster scans
- **📊 SQLite Database** - Track job status (Detected → Notified → Applied) with full history
- **🔍 Multi-Source** - 12+ job sources including Naukri, LinkedIn, Indeed, and global remotes

## 📦 Project Structure

```
job-watchdog/
├── run.py              # Entry point
├── requirements.txt
├── .env.example        # Configuration template
├── data/               # Database & logs
│   ├── jobs.db
│   └── watchdog.log
└── src/
    ├── config/         # Settings & environment
    ├── database/       # Pydantic models & SQLite
    ├── sources/        # Job source implementations
    │   ├── base.py     # Global sources (RemoteOK, etc.)
    │   └── india.py    # India sources (Naukri, LinkedIn, etc.)
    ├── matching/       # Semantic matching engine
    ├── filters/        # LLM experience filtering
    ├── utils/          # Notifications & helpers
    └── orchestrator.py # Main workflow
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
cd job-watchdog
pip install -r requirements.txt

# Download the embedding model (first run)
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

Required settings:
- `TELEGRAM_TOKEN` - From @BotFather
- `TELEGRAM_CHAT_ID` - Your chat ID

Optional but recommended:
- `GROQ_API_KEY` - Free from https://console.groq.com/keys (for LLM filtering)
- `SERPAPI_KEY` - For Google Jobs (250 free/month)

### 3. Run

```bash
python run.py
```

## 🔧 Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TELEGRAM_TOKEN` | Yes | Your Telegram bot token |
| `TELEGRAM_CHAT_ID` | Yes | Your Telegram chat ID |
| `GROQ_API_KEY` | No | Groq API key for LLM filtering (free tier) |
| `SERPAPI_KEY` | No | SerpAPI key for Google Jobs |
| `LOG_LEVEL` | No | DEBUG, INFO, WARNING, ERROR (default: INFO) |

### Customizing Search Profiles

Edit `src/config/settings.py` to modify:
- Target locations
- Excluded job titles (sales, HR, etc.)
- Resume/profile keywords
- Minimum match scores

## 📊 How It Works

```
┌─────────────────┐
│  PHASE 1: Fetch │  Async fetch from 12+ sources
└────────┬────────┘
         ▼
┌─────────────────┐
│  PHASE 2: Dedup │  Remove known jobs (SQLite)
└────────┬────────┘
         ▼
┌─────────────────┐
│  PHASE 3: Match │  Semantic similarity scoring
└────────┬────────┘
         ▼
┌─────────────────┐
│  PHASE 4: Filter│  Location + Title + Experience
└────────┬────────┘
         ▼
┌─────────────────┐
│  PHASE 5: LLM   │  Extract experience, filter seniors
└────────┬────────┘
         ▼
┌─────────────────┐
│  PHASE 6: Save  │  Persist to SQLite
└────────┬────────┘
         ▼
┌─────────────────┐
│  PHASE 7: Alert │  Send to Telegram
└─────────────────┘
```

## 🛡️ Filtering Layers

### Layer 1: Location Filter
- ✅ India cities (Pune, Mumbai, Bangalore, etc.)
- ✅ Remote jobs (no location restriction)
- ❌ USA, UK, Europe, etc.

### Layer 2: Title Filter
- ✅ Data Scientist, Data Analyst, ML Engineer, BI Developer
- ❌ Sales, HR, Marketing, Customer Success, etc.

### Layer 3: Semantic Filter
- Uses sentence embeddings to compare job description vs your profile
- Threshold: 35% similarity (configurable)

### Layer 4: Experience Filter (Quick)
- Regex patterns to detect "Senior", "5+ years", etc.

### Layer 5: LLM Filter (Optional)
- Sends job to Groq/Ollama to extract exact experience requirements
- Filters out roles requiring > 3 years (configurable)

## 📱 Telegram Notifications

Jobs are sent with:
- Match score (%)
- Job category (DS/DA/ML/BI)
- Company name
- Location (with city)
- Direct apply link

## 🔄 Automation

### GitHub Actions (Recommended)

Create `.github/workflows/scan.yml`:

```yaml
name: Job Scan
on:
  schedule:
    - cron: '0 2,8,14,20 * * *'  # Every 6 hours
  workflow_dispatch:

jobs:
  scan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python run.py
        env:
          TELEGRAM_TOKEN: ${{ secrets.TELEGRAM_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          GROQ_API_KEY: ${{ secrets.GROQ_API_KEY }}
```

### Cron (Local)

```bash
# Add to crontab -e
0 8,14,20 * * * cd /path/to/job-watchdog && python run.py >> logs/cron.log 2>&1
```

## 🐛 Troubleshooting

### "sentence-transformers not installed"
```bash
pip install sentence-transformers
```

### "Telegram not working"
1. Make sure you've started a chat with your bot
2. Verify token and chat_id in .env
3. Check: `https://api.telegram.org/bot<TOKEN>/getUpdates`

### "No jobs found"
- Check your internet connection
- Some sources may be rate-limited
- Run with `LOG_LEVEL=DEBUG` for details

## 📈 Future Improvements

- [ ] Add more India sources (AngelList, Wellfound)
- [ ] Company career page scrapers (NVIDIA, Mastercard)
- [ ] Job application tracking UI
- [ ] Auto-apply integration

## 📜 License

MIT License
