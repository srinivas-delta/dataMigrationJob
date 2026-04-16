# DataMigrationJob — BigQuery → PostgreSQL Sync

A production-grade, live-safe sync engine that incrementally migrates tables from **Google BigQuery** to **PostgreSQL** with zero downtime, automatic schema drift handling, watermark tracking, and a REST API for on-demand triggers.

## Quick Start

```bash
# Seed database config from JSON file (one-time)
python main.py --seed-from-json config/tables.json

# List all configurations
python main.py --list-configs

# Run all migrations
python main.py

# Run with parallel workers
python main.py --parallel 4

# Manage configs
python manage_configs.py list
python manage_configs.py add --bq-table project.dataset.table --mode atomic_swap
python manage_configs.py disable project.dataset.table
``` 

---

## Capabilities at a Glance

| Capability | Details |
|---|---|
| **Live-safe upsert** | `INSERT … ON CONFLICT DO UPDATE` — no truncate, no downtime for live users |
| **Incremental sync** | Watermark-based delta fetch — only rows changed since last run are pulled from BQ |
| **Atomic swap** | Zero-downtime full reload — temp table loading + millisecond atomic swap for tables without PK/watermark |
| **Schema drift detection** | Detects added/removed/type-changed columns every run and applies DDL automatically |
| **Row count validation** | Post-migration validation — verifies source rows = target rows |
| **Progress tracking** | Real-time progress with ETA, rows/sec, and percentage complete |
| **Parallel sync** | Run multiple tables concurrently (`--parallel N`) |
| **Multi-table** | Any number of tables in one config file, each with independent settings |
| **REST API** | FastAPI server — trigger syncs via HTTP, check status, view history |
| **Scheduled execution** | GCP Cloud Scheduler → HTTP POST every 2 hours (or any cron) |
| **Sync history** | Postgres-backed `sync_watermarks` + `sync_logs` tables track every run |
| **Retry + recovery** | Exponential backoff on transient errors; row-by-row fallback on batch failures |
| **Full type mapping** | All BigQuery types → PostgreSQL types (JSONB for RECORD/ARRAY/STRUCT, BYTEA, GEOGRAPHY, etc.) |
| **CLI + API** | Run via `python main.py` or start the FastAPI server and call endpoints |

---

## Project Structure

```
dataMigrationJob/
├── api.py                          # FastAPI server (REST API trigger + status)
├── main.py                         # CLI entry point (run directly)
├── requirements.txt
├── .env.example                    # Environment variable template
├── config/
│   └── tables.json                 # Table sync configuration (one entry per table)
└── src/
    ├── bq_to_postgres_migrator.py  # Core: migrate(), migrate_safe(), upsert logic
    ├── schema_drift_handler.py     # Detects & applies schema changes before each sync
    ├── validation.py               # Post-migration validation (row count)
    ├── progress_tracker.py         # Real-time progress tracking
    ├── parallel_sync.py            # Concurrent table migrations
    ├── watermark_store.py          # sync_watermarks + sync_logs table management
    ├── config_loader.py            # Loads .env, GCP credentials, Postgres config
    └── type_mappings.py            # BigQuery → PostgreSQL type conversion table
```

---

## Quick Start

### 1. Install dependencies

```bash
cd dataMigrationJob
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
# PostgreSQL
DB_HOST=your-postgres-host
DB_PORT=5432
DB_NAME=your-database
DB_USER=postgres
DB_PASSWORD=your-password

# GCP credentials — use ONE of:
GCP_SERVICE_ACCOUNT_JSON=/path/to/credentials.json
# GCP_SERVICE_ACCOUNT_B64=<base64-encoded-json>

# API key (required when running api.py)
SYNC_API_KEY=your-secret-key
```

### 3. Configure migrations (Database Config Table)

Migrations are now stored in PostgreSQL, **not** in JSON files. This allows dynamic management without redeployment.

**First time setup — seed from JSON:**

```bash
python main.py --seed-from-json config/tables.json
```

This creates `migration_configs` table and imports all migrations.

**View current configurations:**

```bash
python main.py --list-configs
```

**Add a new migration dynamically:**

```bash
python manage_configs.py add \
  --bq-table project.dataset.accounts \
  --mode safe_upsert \
  --primary-key id \
  --watermark-column updated_at
```

**Disable a migration (without deleting):**

```bash
python manage_configs.py disable project.dataset.accounts
```

[📖 Full config management guide](CONFIG_DB.md)

### 4a. Run via CLI

```bash
python main.py                      # run all enabled migrations from database config  
python main.py --table project.dataset.t   # single table override
python main.py --batch-size 500            # custom batch size (default: None = load all at once)
python main.py --parallel 4                # run 4 tables concurrently
python main.py --no-validate               # skip post-migration validation
python main.py --no-progress               # disable progress bars
python main.py --list-configs              # view all configs from database
```

### 4b. Run via API server

```bash
uvicorn api:app --host 0.0.0.0 --port 8080
# or
python api.py
```

---

## Sync Modes

### `mode: safe_upsert` — Live-safe incremental (recommended)

1. Reads watermark from `sync_watermarks` (last successful sync timestamp)
2. Detects schema drift between BQ and PG, applies DDL changes
3. Fetches only rows where `watermark_column > last_synced_at` from BQ
4. Upserts into PG using `INSERT … ON CONFLICT (primary_key) DO UPDATE`
5. Advances watermark on success

**No truncate. No downtime. Live users unaffected.**

```json
{
  "bq_table": "project.dataset.accounts",
  "mode": "safe_upsert",
  "primary_key": "id",
  "watermark_column": "updated_at",
  "schema_drift": { "removed_policy": "keep" }
}
```

Set `watermark_column: null` to full-fetch on every run (still upserts, no truncate).

### `mode: full` — Drop and recreate (legacy / archive tables only)

Drops the PG table and reloads everything from BQ. Use only for non-live reference tables.

```json
{
  "bq_table": "project.dataset.audit_archive",
  "mode": "full"
}
```

### `mode: atomic_swap` — Zero-downtime full reload (no PK, no watermark)

For tables that have **no primary key** and **no watermark column**, this mode provides a safe full reload with zero downtime:

1. Creates a temp table (`tablename_tmp`) with the BQ schema
2. Loads ALL data from BigQuery into the temp table
3. **Atomic swap** in a single transaction (~milliseconds):
   - `DROP TABLE main_table CASCADE`
   - `RENAME TABLE temp_table TO main_table`

**Benefits:**
- Old table serves live queries while new data loads
- Switch happens in milliseconds (single PostgreSQL transaction)
- If load fails, main table is untouched
- No duplicate/stale row issues — always a clean slate

```json
{
  "bq_table": "project.dataset.reference_data",
  "mode": "atomic_swap",
  "primary_key": null,
  "watermark_column": null
}
```

**When to use:**
- Tables without a primary key that can't use `safe_upsert`
- Tables without a timestamp/watermark column that can't use `watermark_append`
- Reference/lookup tables that need periodic full refreshes
- When you want guaranteed consistency (no partial updates)

---

## Schema Drift Handling

Before every sync the `SchemaDriftHandler` compares the live BQ schema against the PG table and applies changes automatically:

| Scenario | Action |
|---|---|
| Column added in BQ | `ALTER TABLE ADD COLUMN IF NOT EXISTS` — null-safe, zero downtime |
| Column removed from BQ | Controlled by `removed_policy` (see below) |
| Type changed | Warning logged, skipped — type migrations need manual review to avoid data loss |
| PG table missing | Creates the table from BQ schema (first run / disaster recovery) |

### `removed_policy` options

| Value | What happens |
|---|---|
| `"keep"` **(default)** | PG column is left as-is. Safe — stale but no data loss |
| `"nullify"` | `UPDATE … SET col = NULL` — marks stale without removing |
| `"drop"` | `ALTER TABLE DROP COLUMN` — permanent removal |

Set per table in database config:

```json
"schema_drift": { "removed_policy": "keep" }
```

---

## Data Validation

Post-migration validation runs automatically (unless `--no-validate` is specified) to ensure data integrity:

### Row Count Validation
Compares total row counts between BigQuery source and PostgreSQL target:
```
✅ PASS [row_count] BQ=125,432 vs PG=125,432
```

### Validation Report
After each migration:
```
============================================================
Validation Report: primarycontracts
============================================================
  ✅ PASS [row_count] BQ=125,432 vs PG=125,432

✅ ALL CHECKS PASSED
============================================================
```

Disable validation with `--no-validate` if you need faster syncs for trusted data.

---

## Parallel Sync

Run multiple table migrations concurrently for faster bulk migrations:

```bash
python main.py --parallel 4   # Run 4 tables at a time
```

### How it works
- Tables are distributed across N worker threads
- Each table migration runs independently
- Results are collected and logged to `sync_logs`
- Progress bars are disabled in parallel mode

### When to use
- Multiple small-to-medium tables that can benefit from concurrent I/O
- Initial data load of many tables
- Scheduled batch refreshes

### Thread count recommendations
| Tables | Recommended `--parallel` |
|--------|-------------------------|
| 2-4    | 2 |
| 5-10   | 4 |
| 10+    | 4-8 (depending on DB connections) |


## REST API

Start the server:

```bash
uvicorn api:app --host 0.0.0.0 --port 8080
```

All endpoints require the `X-API-Key` header (set `SYNC_API_KEY` in `.env`).

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness probe — no auth required |
| `POST` | `/api/sync` | Trigger sync for **all** configured tables |
| `POST` | `/api/sync/{table_name}` | Trigger sync for **one** table (short name, e.g. `accounts`) |
| `GET` | `/api/sync/status` | Current watermark + status per table |
| `GET` | `/api/sync/logs` | Run history from `sync_logs` |

All sync endpoints accept `?force_full=true` to bypass the watermark and do a full re-sync.
Syncs run in **background threads** — the API returns immediately, poll `/status` or `/logs` to track.

### Example calls

```bash
# Sync all tables
curl -X POST http://localhost:8080/api/sync \
     -H "X-API-Key: your-secret-key"

# Sync one table
curl -X POST http://localhost:8080/api/sync/accounts \
     -H "X-API-Key: your-secret-key"

# Force full re-sync (ignores watermark)
curl -X POST "http://localhost:8080/api/sync/accounts?force_full=true" \
     -H "X-API-Key: your-secret-key"

# Current status
curl http://localhost:8080/api/sync/status \
     -H "X-API-Key: your-secret-key"

# Last 20 runs for accounts
curl "http://localhost:8080/api/sync/logs?limit=20&table=accounts" \
     -H "X-API-Key: your-secret-key"
```

Interactive docs at: `http://localhost:8080/docs`

---

## Scheduling (Every 2 Hours via GCP Cloud Scheduler)

1. Deploy the API server (Cloud Run, GCE, or any host)
2. Create a Cloud Scheduler job:

| Setting | Value |
|---|---|
| Schedule | `0 */2 * * *` |
| Target type | HTTP |
| URL | `https://your-domain/api/sync` |
| HTTP method | POST |
| Headers | `X-API-Key: your-secret-key` |
| Auth | OIDC (service account) or just the API key |
| Retry | 3 attempts, 60s min backoff |

Cloud Scheduler free tier covers 3 jobs — no extra cost.

---

## Sync State Tables (auto-created in Postgres)

### `sync_watermarks` — current state per table

| Column | Description |
|---|---|
| `table_name` | Short table name (PK) |
| `bq_table_ref` | Full BQ reference |
| `last_synced_at` | Timestamp of last **successful** sync |
| `status` | `never_run` / `running` / `success` / `failed` |
| `last_row_count` | Rows upserted in last run |
| `last_error` | Error message if last run failed |

### `sync_logs` — full run history

| Column | Description |
|---|---|
| `id` | Auto-increment PK |
| `table_name` | Table that was synced |
| `status` | `success` / `failed` / `skipped` |
| `rows_affected` | Rows upserted |
| `columns_added` | Schema drift: columns added |
| `columns_dropped` | Schema drift: columns handled by policy |
| `type_mismatches` | Schema drift: type warnings |
| `duration_secs` | Wall-clock time |
| `drift_summary` | Human-readable drift report |
| `error` | Error detail if failed |
| `started_at` / `finished_at` | Timestamps |

---

## Multi-Table Configuration Reference

Configurations are now stored in the `migration_configs` PostgreSQL table. Here are examples of how to add different migration types:

### Example 1: Incremental upsert with watermark

```bash
python manage_configs.py add \
  --bq-table project.dataset.accounts \
  --mode safe_upsert \
  --primary-key id \
  --watermark-column updated_at \
  --description "Incremental upsert — only changed rows fetched"
```

### Example 2: Full reload with atomic swap

```bash
python manage_configs.py add \
  --bq-table project.dataset.reference_data \
  --mode atomic_swap \
  --description "Full reload with atomic table swap (no downtime)"
```

### Example 3: Full table refresh (legacy archive)

```bash
python manage_configs.py add \
  --bq-table project.dataset.audit_archive \
  --mode full \
  --description "Archive table — full drop-reload"
```

### Example 4: Lookup table with nullified stale columns

```bash
python manage_configs.py add \
  --bq-table project.dataset.countries \
  --mode safe_upsert \
  --primary-key country_code \
  --description "Lookup table with nullified removed columns"
```

Then update the schema_drift policy via SQL:

```sql
UPDATE migration_configs
SET schema_drift = '{"removed_policy": "nullify"}'::jsonb
WHERE bq_table = 'project.dataset.countries';
```

[📖 Full configuration guide](CONFIG_DB.md)

---

## GCP Credentials Setup

### Option 1 — Service account JSON file

```bash
export GCP_SERVICE_ACCOUNT_JSON=/path/to/service-account.json
```

### Option 2 — Base64 encoded (CI/CD, Cloud Run)

```bash
export GCP_SERVICE_ACCOUNT_B64=$(cat service-account.json | base64)
```

### Option 3 — Local file (dev only)

Place `seismic-*.json` in the project root — auto-detected.

---

## Type Mapping Reference

| BigQuery Type | PostgreSQL Type |
|---|---|
| INT, INT64, INTEGER | BIGINT |
| TINYINT, BYTEINT, SMALLINT | SMALLINT |
| FLOAT64, DOUBLE | DOUBLE PRECISION |
| FLOAT32 | REAL |
| NUMERIC, DECIMAL | NUMERIC |
| BIGNUMERIC | NUMERIC |
| STRING, TEXT | TEXT |
| VARCHAR | VARCHAR(255) |
| BOOL, BOOLEAN | BOOLEAN |
| DATE | DATE |
| TIME | TIME |
| DATETIME | TIMESTAMP |
| TIMESTAMP, TIMESTAMPTZ | TIMESTAMP WITH TIME ZONE |
| RECORD, STRUCT | JSONB |
| ARRAY | JSONB |
| JSON, OBJECT, MAP | JSONB |
| BYTES, BINARY | BYTEA |
| GEOGRAPHY | GEOGRAPHY (requires PostGIS) |
| INTERVAL, DURATION | INTERVAL |
| `mode: REPEATED` (any type) | JSONB |
| `mode: REQUIRED` (any type) | `<type> NOT NULL` |
| Unknown type | TEXT (safe default) |

---

## Troubleshooting

### "No valid Google service account credentials found"
Set `GCP_SERVICE_ACCOUNT_JSON` or `GCP_SERVICE_ACCOUNT_B64` in `.env`, or place `seismic-*.json` in the project root.

### "ON CONFLICT" error during upsert
The `primary_key` you specified must have a **UNIQUE constraint** in Postgres. Add it manually if missing:
```sql
ALTER TABLE accounts ADD CONSTRAINT accounts_pkey PRIMARY KEY (id);
```

### Sync stuck on "running"
A previous run may have crashed mid-flight. Force reset via:
```sql
UPDATE sync_watermarks SET status = 'failed' WHERE table_name = 'accounts';
```
Then re-trigger.

### High BQ cost
Ensure `watermark_column` is set — this turns a full table scan into a filtered delta query. BQ charges per bytes scanned, so filtering to only changed rows dramatically reduces cost.

### Batch size errors / memory issues
Reduce `batch_size` in `config/tables.json`:
```json
{ "batch_size": 250 }
```

---

## Python API Usage

```python
from src.bq_to_postgres_migrator import BQToPostgresMigrator
from src.config_loader import ConfigLoader

gcp_creds = ConfigLoader.load_gcp_credentials()
pg_config = ConfigLoader.load_postgres_config()
pg_conn = ConfigLoader.build_postgres_connection_string(pg_config)

migrator = BQToPostgresMigrator(
    bq_table_ref='project.dataset.accounts',
    gcp_credentials=gcp_creds,
    pg_connection_string=pg_conn,
    primary_key='id',
    removed_policy='keep',   # 'keep' | 'nullify' | 'drop'
)

# Safe incremental upsert (no truncate)
result = migrator.migrate_safe(
    batch_size=1000,
    watermark_col='updated_at',
    watermark_value='2026-03-15T10:00:00',  # None = full fetch
)

# Legacy full drop-reload
result = migrator.migrate(batch_size=1000)
```

