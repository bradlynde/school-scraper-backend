# LOE Generator

Engagement Letter Generator for Nonprofit Security Advisors. Deploy to Railway or Vercel.

## Setup

1. `npm install`
2. Add `OPENAI_API_KEY` to environment variables
3. `npm run build && npm start`

## Deployment

- **Railway**: Connect this branch; set Root Directory to `npsa-scraper`; add `OPENAI_API_KEY`
- **Vercel**: Import project; set Root Directory to `npsa-scraper`; add `OPENAI_API_KEY`

The main NPSA frontend loads this app via iframe when `NEXT_PUBLIC_LOE_API_URL` is set.
