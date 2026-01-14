# Lyftr AI – Backend Assignment (Containerized Webhook API)

## Tech Stack
- Python 3.11
- FastAPI (async)
- SQLite (aiosqlite)
- Docker & Docker Compose

## How to Run

```bash
export WEBHOOK_SECRET="testsecret"
export DATABASE_URL="sqlite:////data/app.db"
make up
