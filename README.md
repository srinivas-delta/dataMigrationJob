# BigQuery to PostgreSQL Migration Job

A standalone, reusable tool to migrate tables from Google BigQuery to PostgreSQL with automatic schema conversion.

## 🚀 Quick Start

### Installation

```bash
cd bq-migration-job
pip install -r requirements.txt
```

### Setup Configuration

1. **Copy environment file:**
```bash
cp .env.example .env
```

2. **Edit `.env` with your settings:**
```bash
DB_HOST=your-postgres-host
DB_PORT=5432
DB_NAME=your-database
DB_USER=postgres
DB_PASSWORD=your-password

# Google Cloud - use ONE of:
GCP_SERVICE_ACCOUNT_JSON=/path/to/credentials.json
# OR
GCP_SERVICE_ACCOUNT_B64=<base64-encoded-json>
```

3. **Configure tables in `config/tables.json`:**
```json
{
  "migrations": [
    {
      "bq_table": "project.dataset.table1"
    },
    {
      "bq_table": "project.dataset.table2"
    }
  ],
  "batch_size": 1000
}
```

### Run Migration

**Using config file:**
```bash
python main.py
```

**Single table:**
```bash
python main.py --table project.dataset.table
```

**Custom batch size:**
```bash
python main.py --batch-size 500
```

**Custom config file:**
```bash
python main.py --config path/to/config.json
```

## 📋 Features

✅ **Standalone** - Works independently, no dependencies on vision-bi-backend  
✅ **Configurable** - Easy config files or command-line arguments  
✅ **Type Conversion** - Automatic BigQuery → PostgreSQL type mapping  
✅ **Batch Processing** - Efficient data transfer in configurable batches  
✅ **Drop & Recreate** - Always starts fresh (drops existing table)  
✅ **Progress Tracking** - Detailed logging of each migration step  

## 📁 Directory Structure

```
bq-migration-job/
├── main.py                  # Main entry point
├── requirements.txt         # Python dependencies
├── .env.example            # Environment template
├── config/
│   └── tables.json         # Migration configuration
├── src/
│   ├── __init__.py
│   ├── bq_to_postgres_migrator.py    # Core migration logic
│   └── config_loader.py               # Configuration management
└── README.md               # This file
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | localhost |
| `DB_PORT` | PostgreSQL port | 5432 |
| `DB_NAME` | PostgreSQL database | postgres |
| `DB_USER` | PostgreSQL user | postgres |
| `DB_PASSWORD` | PostgreSQL password | (empty) |
| `GCP_SERVICE_ACCOUNT_JSON` | Path to GCP credentials file | - |
| `GCP_SERVICE_ACCOUNT_B64` | Base64 encoded GCP credentials | - |

### Config File Format (config/tables.json)

```json
{
  "migrations": [
    {
      "bq_table": "project.dataset.table",
      "pg_table": "optional_custom_name"
    }
  ],
  "batch_size": 1000,
  "postgres": {
    "host": "localhost",
    "port": "5432",
    "name": "dbname",
    "user": "user",
    "password": "pass"
  }
}
```

## 🔐 GCP Credentials Setup

### Option 1: Service Account JSON File

```bash
export GCP_SERVICE_ACCOUNT_JSON=/path/to/service-account.json
python main.py
```

### Option 2: Base64 Encoded (for CI/CD)

```bash
# Encode credentials
cat service-account.json | base64 -w 0 > credentials.b64

# Use in environment
export GCP_SERVICE_ACCOUNT_B64=$(cat credentials.b64)
python main.py
```

### Option 3: Local File (Development)

Place `seismic-*.json` in the current directory:
```bash
cp /path/to/seismic-helper-364013-*.json ./
python main.py
```

## 📊 Type Mapping

| BigQuery | PostgreSQL |
|----------|-----------|
| INT64 | BIGINT |
| FLOAT64 | DOUBLE PRECISION |
| STRING | TEXT |
| BOOLEAN | BOOLEAN |
| DATE | DATE |
| DATETIME | TIMESTAMP |
| TIMESTAMP | TIMESTAMP WITH TIME ZONE |
| NUMERIC | NUMERIC |
| RECORD/STRUCT | JSONB |
| ARRAY | JSONB |
| BYTES | BYTEA |

## 📖 Usage Examples

### Example 1: Migrate Single Table

```bash
python main.py --table seismic-helper-364013.vision_lookup.accounts
```

### Example 2: Migrate Multiple Tables from Config

**config/tables.json:**
```json
{
  "migrations": [
    { "bq_table": "seismic-helper-364013.vision_lookup.accounts" },
    { "bq_table": "seismic-helper-364013.vision_lookup.companies" },
    { "bq_table": "seismic-helper-364013.vision3_source.transactions" }
  ],
  "batch_size": 2000
}
```

```bash
python main.py
```

### Example 3: Custom PostgreSQL Batch Size

```bash
python main.py --batch-size 500
```

### Example 4: Use Different Config File

```bash
python main.py --config config/prod-tables.json
```

## 🐛 Troubleshooting

### "No valid Google service account credentials found"

**Solution:** Set one of these:
- `GCP_SERVICE_ACCOUNT_JSON` environment variable
- `GCP_SERVICE_ACCOUNT_B64` environment variable  
- Place `seismic-*.json` in current directory

### "PostgreSQL connection failed"

**Solution:** Verify connection settings in `.env`:
```bash
# Test connection
psql -h $DB_HOST -U $DB_USER -d $DB_NAME
```

### "Table not found in BigQuery"

**Solution:** Verify table reference format:
```
correct:   project.dataset.table
incorrect: project/dataset/table or project:dataset.table
```

### "Server closed the connection"

**Solution:** Reduce batch size in config:
```json
{
  "batch_size": 500
}
```

## 📈 Performance Tuning

### Batch Size
- **Smaller (500)** - More stable, better for large text fields
- **Default (1000)** - Good balance
- **Larger (5000)** - Faster for numeric data, may use more memory

### Connection Pooling
The tool uses SQLAlchemy's default connection pooling. For very large migrations:
```python
# Modify connection string in main.py if needed
# postgresql+psycopg2://user:pass@host/db?pool_size=10&max_overflow=20
```

## 🔄 Automation

### Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "main.py"]
```

Build and run:
```bash
docker build -t bq-migration-job .
docker run -e GCP_SERVICE_ACCOUNT_B64="$CREDS" \
           -e DB_HOST="postgres-host" \
           bq-migration-job
```

### Scheduled Jobs (Cron)

```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/bq-migration-job && python main.py >> migration.log 2>&1
```

### GitHub Actions

```yaml
name: BQ Migration

on:
  schedule:
    - cron: '0 2 * * *'

jobs:
  migrate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python main.py
        env:
          GCP_SERVICE_ACCOUNT_B64: ${{ secrets.GCP_CREDENTIALS }}
          DB_HOST: ${{ secrets.DB_HOST }}
          DB_USER: ${{ secrets.DB_USER }}
          DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
```

## 📝 Logging

The tool provides detailed output for each migration:

```
================================================================
Starting migration: project.dataset.table → table
================================================================

📋 Step 1: Fetching BigQuery table schema...
   Found 47 columns

🔧 Step 2: Generating PostgreSQL DDL...
   DDL generated:
   CREATE TABLE "table" (
       "id" BIGINT NOT NULL,
   ...

🗑️  Step 3: Dropping existing PostgreSQL table...
✅ Dropped table 'table' (if existed)

🏗️  Step 4: Creating PostgreSQL table...
✅ Created table 'table'

📦 Step 5: Transferring data...
📊 Fetching data from BigQuery: project.dataset.table
  Transferred 1000 rows...
  Transferred 2000 rows...
✅ Transferred 2226 rows to 'table'

================================================================
✅ Migration completed successfully!
   Rows transferred: 2226
   Duration: 11.71 seconds
================================================================
```

## 📚 API Reference

### BQToPostgresMigrator

```python
from src.bq_to_postgres_migrator import BQToPostgresMigrator
from src.config_loader import ConfigLoader

# Load credentials and config
gcp_creds = ConfigLoader.load_gcp_credentials()
pg_config = ConfigLoader.load_postgres_config()
pg_conn = ConfigLoader.build_postgres_connection_string(pg_config)

# Create migrator
migrator = BQToPostgresMigrator(
    bq_table_ref='project.dataset.table',
    gcp_credentials=gcp_creds,
    pg_connection_string=pg_conn
)

# Run migration
result = migrator.migrate(batch_size=1000)

print(f"Migrated {result['rows_transferred']} rows in {result['duration_seconds']:.2f}s")
```

## 📄 License

This is a standalone tool for internal use. Modify as needed.

## ❓ Questions?

For issues or questions:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review the config files
3. Check environment variables: `env | grep -E "DB_|GCP_"`
