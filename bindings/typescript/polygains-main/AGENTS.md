# PolyGains - AI Agent Guide

## Project Overview

PolyGains is a high-performance analytics platform for tracking insider trading patterns and market data on Polymarket. It streams blockchain events from Polygon Mainnet via Subsquid, detects suspicious trading patterns, and provides a web dashboard for real-time market insights.

**Key Technologies:**
- **Runtime:** Bun (not Node.js)
- **Language:** TypeScript
- **Database:** PostgreSQL with Drizzle ORM
- **Frontend:** Preact (React-compatible) + TailwindCSS + DaisyUI
- **Process Management:** PM2
- **Testing:** Bun test + Playwright (E2E)
- **Linting:** Biome (not ESLint/Prettier)

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Frontend (Port 4033)                  │
│  Bun.serve() + Preact + TailwindCSS + DaisyUI + SWR     │
└──────────────────┬──────────────────────────────────────┘
                   │ Proxies API calls
                   ▼
┌─────────────────────────────────────────────────────────┐
│                  API Server (Port 4069)                  │
│              Bun.serve() REST API + CORS                 │
└──────────────────┬──────────────────────────────────────┘
                   │ Reads from
                   ▼
┌─────────────────────────────────────────────────────────┐
│                Postgres (Port 5469)                      │
│              Docker Container                            │
└──────────────────┬──────────────────────────────────────┘
                   │ Written to by
                   ▼
         ┌─────────────────────┬─────────────────────┐
         │                     │                     │
         ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Pipeline   │    │   Markets    │    │ BloomFilter/ │
│              │    │   Service    │    │   Detector   │
│ Processes    │    │              │    │              │
│ blockchain   │    │ Fetches      │    │ Detects      │
│ events from  │    │ Polymarket   │    │ insiders     │
│ Subsquid     │    │ CLOB API     │    │              │
└──────────────┘    └──────────────┘    └──────────────┘
```

## Directory Structure

```
polygains/
├── src/
│   ├── lib/                    # Utilities, types, database schema
│   │   ├── db/                 # Database: schema, migrations, queries
│   │   ├── types/              # TypeScript type definitions
│   │   ├── abi.ts              # Contract ABIs
│   │   ├── const.ts            # Constants (START_BLOCK, contracts, etc.)
│   │   ├── hash.ts             # Wallet hashing utilities
│   │   ├── hashset.ts          # XXHash32Set implementation
│   │   ├── parser.ts           # Blockchain event parsers
│   │   └── utils.ts            # General utilities
│   ├── services/               # Background services
│   │   ├── server.ts           # REST API server (Bun.serve)
│   │   ├── markets.ts          # Polymarket CLOB data fetcher
│   │   ├── pipe.ts             # Subsquid pipeline processor
│   │   ├── detector.ts         # Insider detection algorithms
│   │   ├── buffer.ts           # Window buffer for aggregations
│   │   ├── filter-persistor.ts # BloomFilter persistence
│   │   └── positions-persistor.ts # Position data persistence
│   └── main.ts                 # Pipeline entry point
├── frontend/                   # React/Preact frontend (separate package)
│   ├── src/
│   │   ├── features/terminal/  # Main terminal feature
│   │   ├── hooks/              # SWR hooks and queries
│   │   ├── context/            # React contexts (UI, Data)
│   │   ├── reducers/           # State reducers
│   │   └── index.ts            # Frontend dev server
│   ├── build.ts                # Production build script
│   └── package.json            # Frontend dependencies
├── drizzle/                    # Database migrations
├── tests/                      # Unit and integration tests
├── integration-tests/          # Playwright E2E tests
├── public/                     # Static assets (favicons, built frontend)
├── docs/                       # Architecture documentation
├── Makefile                    # Primary command interface
└── ecosystem.config.cjs        # PM2 process configuration
```

## Key Configuration Files

| File | Purpose |
|------|---------|
| `package.json` | Root package scripts and dependencies |
| `tsconfig.json` | TypeScript configuration (ESNext, bundler mode) |
| `biome.json` | Linting and formatting rules |
| `drizzle.config.ts` | Database migration configuration |
| `ecosystem.config.cjs` | PM2 process definitions |
| `compose.yml` | PostgreSQL Docker service |
| `.env` | Local environment variables (not committed) |
| `.env.local.example` | Template for environment setup |

## Build and Development Commands

All commands are managed through the Makefile:

```bash
# Start everything (postgres + api + markets + pipeline + frontend)
make start

# Stop all services
make stop

# View service status
make status

# View logs
make logs              # All logs
make logs-api          # API server only
make logs-markets      # Markets service only
make logs-pipeline     # Pipeline only
make logs-frontend     # Frontend only
make logs-db           # Postgres only

# Development (manual control)
make dev-local         # Setup postgres, show manual run commands
make run-server        # Run API server only (port 4069)
make run-markets       # Run markets service only
make run-pipeline      # Run pipeline only
make run-frontend      # Run frontend only (port 4033)

# Database
make db-generate       # Generate Drizzle migrations
make db-migrate        # Apply migrations
make db-prepare        # Generate + migrate
make db-reset          # Reset database (WARNING: deletes data)
make db-shell          # Open psql shell

# Testing
make test              # Run unit tests only
make test-e2e          # Run Playwright E2E tests
make test-all          # Run all tests

# Other
make build-frontend    # Build frontend for production
make deploy-frontend   # Deploy to Cloudflare Pages
make clean             # Clean docker resources and processes
```

## Bun-Specific Conventions

**ALWAYS use Bun, never Node.js:**

| Task | Bun Command | ❌ Don't Use |
|------|-------------|--------------|
| Run TypeScript | `bun src/main.ts` | `node`, `ts-node` |
| Install deps | `bun install` | `npm install`, `yarn` |
| Run scripts | `bun run <script>` | `npm run` |
| Execute package | `bunx <package>` | `npx` |
| Testing | `bun test` | `jest`, `vitest` |
| Build | `bun build.ts` | `webpack`, `esbuild` |

**Key Bun APIs:**
- `Bun.serve()` - HTTP server with routes, WebSockets
- `Bun.sql()` - PostgreSQL client (built-in)
- `Bun.file()` - File I/O (prefer over `node:fs`)
- `Bun.$``` - Shell execution (prefer over `execa`)
- `bun:sqlite` - SQLite (built-in)

**Environment Variables:**
- Bun automatically loads `.env` files - **don't use `dotenv`**
- Frontend env vars must be prefixed with `BUN_PUBLIC_` to be inlined

## Code Style Guidelines

**Linting:** Biome (configured in `biome.json`)

```bash
# Check linting
bun run lint

# Fix linting issues
bun run lint:fix
```

**Key Style Rules:**
- Use double quotes for strings
- Strict TypeScript enabled
- Path alias: `@/*` maps to `src/*`
- Unused variables allowed (for development convenience)

**Import Patterns:**
```typescript
// Use path aliases
import { something } from "@/lib/utils";
import { schema } from "@/lib/db/schema";

// Prefer Bun APIs
import { serve } from "bun";
```

## Database Schema

Key tables:
- `markets` - Polymarket market data
- `market_tokens` - Token outcomes per market
- `token_market_lookup` - Token ID to market mapping
- `token_stats` - Aggregated token statistics
- `insider_positions` - Persisted insider positions (hashed)
- `detected_insiders` - Detected insider addresses
- `account_stats` - Account trading statistics
- `checkpoint` - Stream processing cursor
- `detector_snapshots` - Insider detector state

**Database Operations:**
```bash
# Generate migrations after schema changes
make db-generate

# Apply migrations
make db-migrate

# Full reset (WARNING: data loss)
make db-reset
```

## Testing

**Unit Tests:** Bun test framework
```bash
# Run all unit tests
make test

# Run specific test
bun test tests/parser.test.ts
```

**E2E Tests:** Playwright
```bash
# Ensure services are running first
make start

# Run E2E tests
make test-e2e

# Or directly
bunx playwright test
```

**Test File Patterns:**
- Unit: `tests/*.test.ts`
- Integration: `tests/*.integration.test.ts`
- E2E: `integration-tests/*.spec.ts`

## Frontend Architecture

The frontend uses a modern React pattern with:
- **SWR** for server state management
- **Context + Reducers** for client state
- **Feature-based** organization

Key directories:
```
frontend/src/
├── features/terminal/     # Main terminal feature
│   ├── pages/             # Page components
│   ├── components/        # UI components
│   ├── controller/        # useTerminalController hook
│   ├── services/          # backtestEngine, trackerEngine
│   └── selectors/         # Data selectors
├── hooks/                 # Shared hooks
│   ├── queries/           # SWR query hooks
│   └── swr/               # SWR configuration
├── context/               # React contexts
└── reducers/              # State reducers
```

**Build Process:**
```bash
# Development
bun --hot src/index.ts

# Production build (outputs to public/dist)
bun run build
```

## Service Architecture

### API Server (`src/services/server.ts`)
- Bun.serve() with route definitions
- CORS enabled for configured origins
- Endpoints: `/health`, `/stats`, `/markets`, `/alerts`, `/insiders`, etc.
- File-based caching for market data

### Pipeline (`src/main.ts`)
- Subsquid portal integration
- Processes blockchain events from Polygon Mainnet
- Detects insider trading patterns
- Persists to database

### Markets Service (`src/services/markets.ts`)
- Fetches Polymarket CLOB API data
- Runs on interval (default: 1 hour)
- Upserts market data to database

### Process Management
All services run under PM2 (defined in `ecosystem.config.cjs`):
- `api-server` - REST API
- `markets` - Market data fetcher
- `pipeline` - Blockchain processor
- `frontend` - Dev server

## Environment Variables

Copy `.env.local.example` to `.env` and configure:

```bash
# PostgreSQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres
POSTGRES_PORT=5469
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5469/postgres

# API Server
API_HOST=127.0.0.1
API_PORT=4069

# Frontend
FRONTEND_HOST=127.0.0.1
FRONTEND_PORT=4033
API_UPSTREAM_BASE_URL=http://127.0.0.1:4069
BUN_PUBLIC_API_BASE_URL=http://127.0.0.1:4069

# Services
FETCH_INTERVAL_MS=3600000  # 1 hour
NODE_ENV=development
```

## Important Implementation Details

### Insider Detection
- Uses `XXHash32Set` for fast lookups (replaces BloomFilter)
- Wallets are hashed for privacy
- Detection based on volume thresholds and price movements

### Database Connection
- Uses Drizzle ORM with PostgreSQL
- Supports both TCP and Unix socket connections
- Connection pooling handled automatically

### State Persistence
- Pipeline state saved to `state.json` and database
- Detector snapshots saved incrementally
- Allows recovery after restart

### CORS Configuration
API server supports configurable CORS origins via `CORS_ORIGINS` env var (comma-separated).

## Security Considerations

1. **Wallet Privacy:** Wallet addresses are hashed before storage (except in `accountWalletMap`)
2. **CORS:** API validates origins against allowlist
3. **SQL Injection:** Protected by Drizzle ORM parameterized queries
4. **HTTPS:** Production API redirects HTTP to HTTPS via `x-forwarded-proto` header

## Troubleshooting

```bash
# Check if postgres is running
docker compose ps

# View service status
make status

# Reset everything (WARNING: data loss)
make clean
make start

# Port conflicts
lsof -i :4069  # API port
lsof -i :4033  # Frontend port
lsof -i :5469  # Postgres port
```

## References

- `CLAUDE.md` - Bun-specific coding guidelines
- `docs/DESIGN.md` - Frontend architecture design
- `BACKTEST.md` - Backtest implementation notes
- `README.md` - General project documentation
