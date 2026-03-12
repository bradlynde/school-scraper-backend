# NPSA Auth Service

Centralized authentication for NPSA tools (School Scraper, Church Scraper, LOE Generator).

- **POST /login** - Authenticate with username/password, returns JWT
- **GET /health** - Health check

## Setup

1. Add `JWT_SECRET` (shared with all backends)
2. Add `DATABASE_URL` (Railway Postgres)
3. On first deploy, `init_db()` creates `users` table and seeds user `Koen` / `admin`

## Adding users

Connect to Postgres and run:

```sql
INSERT INTO users (username, password_hash) 
VALUES ('username', '<bcrypt_hash>');
```

Or add an admin endpoint for user creation (not included by default).
