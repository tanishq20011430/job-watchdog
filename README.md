# 🤖 Job Watchdog - Automated Job Alert System

Get instant Telegram notifications for fresh job postings matching your profile!

## Features

- 🔍 **14 Job Sources** - Indeed, Google Jobs, Naukri, RemoteOK, and more
- ⚡ **Fresh Jobs Priority** - Focuses on jobs posted within last 24-72 hours
- 🎯 **Smart Matching** - Hybrid keyword + TF-IDF scoring system
- 📱 **Telegram Alerts** - Instant notifications with job details
- ⏰ **Automated** - Runs every 6 hours via GitHub Actions
- 📊 **API Quota Management** - Tracks SerpAPI usage (250/month limit)

## Job Sources

| Source | Type | Freshness |
|--------|------|-----------|
| Google Jobs (SerpAPI) | API | Last 24 hours |
| Naukri via Google | Indexed | Recent |
| Indeed India | Playwright | Last 3 days |
| RemoteOK | API | Daily |
| Arbeitnow | API | Daily |
| Findwork | API | Daily |
| Himalayas | API | Daily |
| Jobicy | API | Daily |
| TheMuse | API | Various |
| WeWorkRemotely | RSS | Daily |
| LandingJobs | API | Recent |
| HN Hiring | API | Monthly |
| DuckDuckGo | Search | Various |

## Setup

### 1. Create Telegram Bot

1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Send `/newbot` and follow instructions
3. Copy your **Bot Token**
4. Send a message to your bot to activate it
5. Get your **Chat ID** from: `https://api.telegram.org/bot<TOKEN>/getUpdates`

### 2. Get SerpAPI Key (Optional)

1. Sign up at [SerpAPI](https://serpapi.com/)
2. Get your API key (100 free searches/month)

### 3. Local Setup

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/job-watchdog.git
cd job-watchdog

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Create .env file
cat > .env << EOF
TELEGRAM_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_chat_id
SERPAPI_KEY=your_serpapi_key
EOF

# Run
python job_watchdog.py
```

### 4. GitHub Actions Setup (Automated)

1. Fork this repository
2. Go to **Settings → Secrets and variables → Actions**
3. Add these secrets:
   - `TELEGRAM_TOKEN` - Your Telegram bot token
   - `TELEGRAM_CHAT_ID` - Your Telegram chat ID
   - `SERPAPI_KEY` - Your SerpAPI key

The workflow runs automatically every 6 hours, or trigger manually from the **Actions** tab.

## Configuration

Edit `job_watchdog.py` to customize:

```python
# Your resume profiles
DS_PROFILE = """
Your Data Scientist profile here...
"""

DA_PROFILE = """
Your Data Analyst profile here...
"""

# Search configurations
SEARCH_CONFIGS = [
    {"keywords": ["data scientist", "machine learning"], "profile": DS_PROFILE, "tag": "Data Science"},
    {"keywords": ["data analyst", "business analyst"], "profile": DA_PROFILE, "tag": "Data Analytics"},
]

# Minimum match score (0-100)
MIN_MATCH_SCORE = 15
```

## API Usage

- **SerpAPI**: 250 calls/month (tracked automatically)
- Each run uses ~2 API calls
- That's ~125 runs/month or ~4 runs/day

## Notifications

You'll receive Telegram messages like:

```
🎯 New Job Match! (Score: 35.2%)

📌 Senior Data Scientist
🏢 Amazon
📍 Bangalore, India
🕐 Posted: 2 hours ago

🔗 Apply: https://...
```

## License

MIT License

## Author

Built with ❤️ for job seekers
