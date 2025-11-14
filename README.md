# Real-Time Distributed Task Network

This project demonstrates a real-time, credit-based marketplace where customers post data-processing tasks and distributed workers claim and complete individual chunks. A modern React + TypeScript frontend coordinates with an Express/LowDB backend to manage task lifecycles, wallet balances, bucket assignments, and live progress reporting.

## Project Vision

- **Customers** upload datasets, define processing requirements, and pre-fund work. Tasks are split into discrete chunks that can be processed independently.
- **Workers** connect to the network, claim work buckets, run custom code against assigned chunks, and earn credits upon successful completion.
- **Bucket Orchestration** ensures concurrency is capped, tracking which chunks are in progress versus completed, while redistributing buckets as workers finish.
- **Wallet & Fees** give customers a tracked balance, debit them per completed chunk, and allocate platform/worker revenue splits.

The result is a mini distributed computation marketplace suitable for experimentation with task scheduling, progress tracking, and real-time worker orchestration.

## Key Features

- **Task Creation UI** for customers to configure tasks, budgets, and required capabilities, with live progress dashboards.
- **Worker Dashboard** showing claimed tasks, bucket progress, latest results, and credit earnings in near real time.
- **Wallet Sandbox Mode** with deposit/withdraw flows and Stripe integration fallback.
- **Chunk Assignment API** managing bucket concurrency, progress updates, item previews, and payouts.
- **Worker Runner Script** automating polling, chunk processing, progress heartbeats, and result submission.
- **Persistent Storage** using LowDB JSON files for tasks, results, assignments, and transactions.

## Directory Structure

```
backend/
  server.js          # Express API orchestrating tasks, workers, wallet, and bucket lifecycle
  db.js              # LowDB initialization, helpers for reading/writing JSON storage
  data/
    database.json    # Default database content for seeded demo
  storage/           # Uploaded task artifacts (code.zip, data.json) and worker results
scripts/
  worker-runner.mjs  # CLI worker client that polls, executes, and reports chunk results
src/
  App.tsx            # Main React application tying together customer/worker tabs and live data
  main.tsx           # React entry bootstrap wiring in providers and root render
  styles.css         # Tailored styling for dashboard layout, tables, and progress visuals
index.html           # Vite entry page injecting React root
vite.config.ts       # Vite configuration for dev/build tooling
```

## Backend Overview (`backend/`)

- **`server.js`**
  - Hosts REST API (`/api/tasks`, `/api/worker/*`, `/api/wallet/*`) and serves storage assets.
  - Manages task budget calculations, chunk allocations (`/api/worker/next-chunk`), progress recording, fee payouts, and heartbeat tracking for workers.
  - Normalizes bucket configuration, assigns work considering concurrency limits, and persists state via LowDB.

- **`db.js`**
  - Initializes LowDB, exposes `getDb`, `saveDb`, and ensures default collections exist (tasks, chunkResults, assignments, users, wallets).
  - Provides utilities to interact with the JSON-backed data store used by the API.

- **`data/database.json`**
  - Sample dataset demonstrating task/worker structure; seeded at startup for quick experimentation.

- **`storage/`**
  - Contains per-task folders storing uploaded `code.zip`, `data.json`, and generated results for buckets.

## Frontend Overview (`src/`)

- **`App.tsx`**
  - Core SPA with tabbed views for customer and worker personas.
  - Manages state for task lists, selections, real-time bucket summaries, wallet transactions, and worker metrics.
  - Implements progress visualizations (chunk grids, tables), task detail panes, and wallet flows using fetch-based API calls.

- **`main.tsx`**
  - React bootstrap binding `App` into the DOM, wiring base CSS and third-party providers (e.g., Sonner toasts).

- **`styles.css`**
  - Global styles for layout, buttons, tables, bucket progress bars, worker summaries, and responsive adjustments.

## Supporting Files

- **`scripts/worker-runner.mjs`**: Node-based worker client. Configurable via environment variables (`WORKER_ID`, `API_BASE`, etc.). Automates chunk polling, optional `main.js` execution, progress batching, and chunk result submission.
- **`scripts/run-workers.mjs`**: Convenience launcher that spawns multiple worker runners at once and forwards stdout/stderr for each worker ID.
- **`index.html`**: Minimal HTML shell for Vite to inject the React bundle.
- **`vite.config.ts`**: Vite setup enabling TypeScript, React Fast Refresh, and proxy adjustments if needed.
- **`package.json`**: Declares frontend/backend scripts (`npm run dev`, `npm run worker`) and dependencies (React, Express, LowDB, etc.).
- **`install.sh`**: Convenience script for installing dependencies.

## Workflows

### Customer Flow
1. Navigate to the customer tab.
2. Create a task by uploading code/data assets, defining capabilities, and funding the budget.
3. Monitor progress through the dashboard showing total chunks, bucket slots, assignments, and detailed chunk history.
4. Review wallet transactions and platform fee deductions.

### Worker Flow
1. Run the worker runner: `WORKER_ID=worker-1 API_BASE=http://localhost:4000 node scripts/worker-runner.mjs`, or launch multiple workers with `node scripts/run-workers.mjs worker-1 worker-2`.
2. Worker polls `/api/tasks`, claims assigned tasks, and loops on `/api/worker/next-chunk` for bucket assignments.
3. Processes each chunk (optionally executing uploaded `main.js`) and posts results to `/api/worker/record-chunk` with progress updates.
4. Earns credits automatically when chunks complete and budgets permit.

### Wallet & Payments
- Sandbox mode (`WALLET_SANDBOX_ENABLED=true`) allows direct deposit/withdraw for testing.
- Stripe integration (when configured) provides real payment flow.
- Platform fee percentage is configurable; payouts split between worker and platform wallet entries.

## Getting Started

```bash
# install dependencies
npm install

# start backend + frontend (concurrently via Vite proxy)
npm run dev

# (optional) launch a worker client
WORKER_ID=worker-1 API_BASE=http://localhost:4000 node scripts/worker-runner.mjs

# (optional) launch multiple workers at once
node scripts/run-workers.mjs worker-1 worker-2 worker-3
```

Then open `http://localhost:5173` for the UI. Customers can create tasks and monitor progress; workers will pick up chunks automatically.

## Environment Variables

- `API_BASE`: Base URL for backend API (default `http://localhost:4000`).
- `WORKER_ID`: Unique identifier for each worker client.
- `POLL_INTERVAL`, `HEARTBEAT_INTERVAL`: Worker polling cadence (ms).
- `WALLET_SANDBOX_ENABLED`: Enables sandbox wallet adjustments.
- `PLATFORM_FEE_PERCENT`: Percentage (0–100) of chunk cost reserved for the platform.
- `DEV_INITIAL_WALLET`: Seed wallet balance for new dev users.

Backend `.env` can additionally define Stripe keys and other advanced settings.

## Future Extensions

- Real authentication and role separation.
- WebSocket-based worker/task updates instead of polling.
- Scalable datastore (e.g., PostgreSQL) replacing LowDB for concurrent workloads.
- Enhanced task templates and worker capability matching.
- Production-ready worker sandboxing and code execution safety.

---

This repository acts as a comprehensive example of orchestrating distributed task processing with clear customer/worker workflows, progressive enhancement of bucket orchestration, and a sandbox-friendly development environment.
