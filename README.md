# LOE Generator API (Backend)

Backend service for the LOE (Letter of Engagement) Generator. Provides AI clause polishing via `/api/polish-clause`.

## Setup

1. `npm install`
2. Add `OPENAI_API_KEY` to environment variables
3. `npm run build && npm start`

## API

- **POST /api/polish-clause** — Polishes rough clause text using AI. Body: `{ "text": "..." }`. Returns `{ "polished": "..." }`.

## Deployment

- **Railway**: Connect this branch; add `OPENAI_API_KEY`
- Frontend (Vercel) calls this API when `NEXT_PUBLIC_LOE_API_URL` is set.
