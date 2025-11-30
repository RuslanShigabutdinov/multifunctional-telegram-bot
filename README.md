# Telegram Media Bot

Telegram bot that downloads TikTok/Instagram videos through RapidAPI services, keeps track of request usage, and supports group mention helpers. The project now ships with Docker tooling for easy deployment.

## Local Development

1. Create a `.env` file (see `.env.example`) with:
   ```
   TOKEN=<telegram_bot_token>
   TIKTOK_KEY=<rapidapi_tiktok_key>
   INSTAGRAM_KEY=<rapidapi_instagram_key>
   CHAT_HISTORY_LIMIT=20
   DATABASE_HOST=<postgres_host>
   DATABASE_PORT=5432
   DATABASE_NAME=<database_name>
   DATABASE_USER=<database_user>
   DATABASE_PASSWORD=<database_password>
   ```
   `CHAT_HISTORY_LIMIT` is optional (default 20) and caps how many recent chat messages (user + bot) are kept per chat for Gemini context.
2. Ensure PostgreSQL is running and matches the credentials above (you can `docker-compose up postgres -d` to run only the DB locally).
3. Apply database migrations (after installing dependencies):
   ```bash
   alembic upgrade head
   ```
4. (Optional) Install dependencies and run directly:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   python bot.py
   ```

## Docker

Build the container:

```bash
docker build -t telegram-media-bot .
```

Run the bot, passing the same environment variables:

```bash
docker run --rm \
  --env-file .env \
  -e SKIP_MIGRATIONS=0 \
  -v $(pwd)/usage.json:/app/usage.json \
  telegram-media-bot
```

Mounting `usage.json` is optional but keeps your RapidAPI usage counter persistent across restarts.
The container entrypoint runs `alembic upgrade head` before starting the bot; set `SKIP_MIGRATIONS=1` if you already ran migrations externally.

## Database migrations

- Apply migrations: `alembic upgrade head`
- Create a new migration after editing the schema: `alembic revision -m "my change"`

### docker-compose

If you prefer compose, ensure `usage.json` exists (`touch usage.json`) and run:

```bash
docker-compose up --build -d
```

This will build the image, start the bot, launch the PostgreSQL instance used by the bot, and keep `usage.json` bound into the container for persistent request tracking. Stop it with `docker-compose down`.
