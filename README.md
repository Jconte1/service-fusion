This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.

## Service Fusion OAuth (Client Credentials)

### Environment variables

Set these in your local environment (or `.env.local`):

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/service_fusion
SERVICE_FUSION_CLIENT_ID=your_client_id_here
SERVICE_FUSION_CLIENT_SECRET=your_client_secret_here
ACUMATICA_BASE_URL=https://acumatica.yourcompany.com
ACUMATICA_CLIENT_ID=your_client_id_here
ACUMATICA_CLIENT_SECRET=your_client_secret_here
ACUMATICA_USERNAME=your_username_here
ACUMATICA_PASSWORD=your_password_here
ACUMATICA_COMPANY=your_company_here
ACUMATICA_INVOICE_ENDPOINT=SalesInvoice
SERVICE_FUSION_USE_QUEUE=true
MLD_QUEUE_BASE_URL=https://your-mld-queue-gateway-host
MLD_QUEUE_TOKEN=your_queue_bearer_token
```

### Prisma setup

This project uses Prisma with PostgreSQL.

```bash
npm install
npm run prisma:generate
npm run prisma:push
```

### Local token test endpoint

This project includes a local test route for fetching a Service Fusion token:

- Method: `POST`
- URL: `http://localhost:3000/api/service-fusion/oauth/token`

Notes:

- The route is disabled in production (`404` when `NODE_ENV=production`).
- The route calls `https://api.servicefusion.com/oauth/access_token` with `grant_type=client_credentials`.

### Postman test

1. Start app: `npm run dev`
2. In Postman, send `POST http://localhost:3000/api/service-fusion/oauth/token`
3. Expect JSON response with:
   - `access_token`
   - `token_type`
   - `expires_in`
   - `refresh_token` (if returned by Service Fusion)
   - `expires_at` (computed locally)

### Reusable Service Fusion client

Use `lib/service-fusion/client.ts` for authenticated API calls to `https://api.servicefusion.com/v1`.

- `serviceFusionFetch(path, init)` returns a `Response`.
- `serviceFusionJson<T>(path, init)` returns parsed JSON.

Token handling is automatic:

- Tokens are cached server-side in memory.
- Near expiry, the app tries `refresh_token` first.
- If refresh fails, it falls back to `client_credentials`.
- On `401`, the cache is cleared and one retry is attempted with a new token.

### Daily invoiced jobs extractor (test route)

This project includes a non-production test route to run the daily extraction flow in memory:

- Method: `POST`
- URL: `http://localhost:3000/api/service-fusion/jobs/daily-invoiced`
- Optional date override: `http://localhost:3000/api/service-fusion/jobs/daily-invoiced?date=YYYY-MM-DD`

What it does:

- Builds "today" window in `America/Denver`.
- Pulls paginated `/v1/jobs` where `status=Invoiced` and `updated_date` is within today's Denver window.
- If `date` is supplied, the Denver window is built for that date instead of today.
- Fetches each job detail with `expand=products,services`.
- Fetches each unique customer with `expand=locations,custom_fields`.
- Extracts `Acumatica CustomerID` from customer custom fields.
- Resolves location nickname from customer `locations[]` and stores both:
  - raw job value (`locationNameRaw` from job `location_name`)
  - resolved nickname (`locationNickname`)
- Returns normalized in-memory payload with stats, jobs, and failures.

Notes:

- Route is disabled in production (`404` when `NODE_ENV=production`).
- Includes retry/backoff for Service Fusion `429` and `5xx` responses.

### Daily invoiced jobs DB-ready transform (test route)

This project includes a second non-production test route that returns DB-ready transformed payload
(still in-memory only, no writes):

- Method: `POST`
- URL: `http://localhost:3000/api/service-fusion/jobs/daily-invoiced/db-ready`
- Optional date override: `http://localhost:3000/api/service-fusion/jobs/daily-invoiced/db-ready?date=YYYY-MM-DD`

Transform behavior:

- Services keep `inventoryId` from Service Fusion `service`.
- Products parse `description` (`MODEL - Description`):
  - If parse succeeds: `inventoryId = MODEL`, `description = parsed Description`
  - If parse fails or model missing: `inventoryId = INSPARTS` fallback
- Adds per-line transform issues for fallback cases.

Important:

- This route does not write to DB.
- This route does not call Acumatica.

### Daily invoiced jobs persist to DB (test route)

This project includes a third non-production route that runs extract + transform + DB persistence:

- Method: `POST`
- URL: `http://localhost:3000/api/service-fusion/jobs/daily-invoiced/persist`
- Optional date override: `http://localhost:3000/api/service-fusion/jobs/daily-invoiced/persist?date=YYYY-MM-DD`
- Optional immediate send: `http://localhost:3000/api/service-fusion/jobs/daily-invoiced/persist?date=YYYY-MM-DD&send=true`

What it does:

- Pulls daily invoiced jobs (Denver date window).
- Builds DB-ready transform payload.
- Persists run/job/line/event records via Prisma.
- Persists both location fields on each job record:
  - `locationNameRaw`
  - `locationNickname`
- Marks records `READY` when `Acumatica CustomerID` exists.
- Marks records `FAILED` and stores `failureReason` when required customer mapping is missing.

Important:

- This route writes to your database.
- This route can optionally call Acumatica when `send=true`.

### Daily invoiced jobs send to Acumatica (test route)

This project includes a non-production route for Step 2 (send `READY` jobs from DB to Acumatica):

- Method: `POST`
- URL: `http://localhost:3000/api/service-fusion/jobs/daily-invoiced/send?runId=<RUN_ID>`

What it does:

- Loads `READY` jobs for the provided `runId`.
- Also retries prior jobs that previously failed with `ACUMATICA_SEND_FAILED` events.
- Maps each job into Acumatica invoice payload (`Customer`, `ExtRefNbr`, `LocationID`, `Details[]`, `TaxDetails[]`).
- Sends invoices via queue by default (`SERVICE_FUSION_USE_QUEUE=true`):
  - enqueue `POST {MLD_QUEUE_BASE_URL}/api/erp/jobs/sales-invoices`
  - poll `GET {MLD_QUEUE_BASE_URL}/api/erp/jobs/{jobId}`
- Optional direct fallback when `SERVICE_FUSION_USE_QUEUE=false`:
  - `PUT {ACUMATICA_BASE_URL}/entity/ServiceFusion/24.200.001/{ACUMATICA_INVOICE_ENDPOINT}`
- Marks each job:
  - `SENT` on success (stores `acumaticaRef` when available)
  - `FAILED` on error (stores `failureReason`)
- Writes `SfJobEvent` rows for success/failure with response details.
