# BigQuery to PostgreSQL Type Mappings

This document describes the comprehensive type mapping system used for BigQuery to PostgreSQL migrations.

## Overview

The type mappings are centralized in `src/type_mappings.py` and imported by the migrator. This ensures:
- ✅ Single source of truth for all type conversions
- ✅ Easy to maintain and update mappings
- ✅ Support for all BigQuery and PostgreSQL data types
- ✅ Proper handling of NULL and REPEATED fields

## Usage

### In Migration Code

```python
from src.type_mappings import get_postgresql_type, needs_json_conversion

# Convert a type
pg_type = get_postgresql_type('INT64', 'NULLABLE')  # Returns 'BIGINT'
pg_type = get_postgresql_type('INT64', 'REQUIRED')  # Returns 'BIGINT NOT NULL'
pg_type = get_postgresql_type('STRING', 'REPEATED') # Returns 'JSONB'

# Check if conversion needed
if needs_json_conversion('STRUCT'):
    print("Will be converted to JSONB")
```

## Type Mappings

### Numeric Types

| BigQuery Type | PostgreSQL Type | Notes |
|---------------|-----------------|-------|
| INT, INT64, INTEGER | BIGINT | Standard integer |
| TINYINT, BYTEINT, SMALLINT | SMALLINT | Small integer |
| BIGINT | BIGINT | Large integer |
| FLOAT, FLOAT32 | REAL | Single precision |
| FLOAT64, DOUBLE | DOUBLE PRECISION | Double precision |
| NUMERIC, DECIMAL | NUMERIC | Precise decimal |
| BIGNUMERIC, BIGDECIMAL | NUMERIC | High precision decimal |

### String Types

| BigQuery Type | PostgreSQL Type | Notes |
|---------------|-----------------|-------|
| STRING | TEXT | Variable length text |
| VARCHAR | VARCHAR(255) | Variable character |
| CHAR | CHAR | Fixed character |
| TEXT | TEXT | Large text |

### Binary Types

| BigQuery Type | PostgreSQL Type | Notes |
|---------------|-----------------|-------|
| BYTES | BYTEA | Binary data |
| BINARY | BYTEA | Binary data |
| VARBINARY | BYTEA | Variable binary |

### Boolean Type

| BigQuery Type | PostgreSQL Type | Notes |
|---------------|-----------------|-------|
| BOOL, BOOLEAN | BOOLEAN | True/False |

### Date & Time Types

| BigQuery Type | PostgreSQL Type | Notes |
|---------------|-----------------|-------|
| DATE | DATE | Date only |
| TIME | TIME | Time only |
| DATETIME | TIMESTAMP | Date and time (no timezone) |
| TIMESTAMP, TIMESTAMPTZ | TIMESTAMP WITH TIME ZONE | Date, time, and timezone |

### Complex Types (Converted to JSONB)

| BigQuery Type | PostgreSQL Type | Notes |
|---------------|-----------------|-------|
| RECORD, STRUCT | JSONB | Nested object → JSON |
| ARRAY | JSONB | Array → JSON |
| JSON | JSONB | JSON → PostgreSQL JSON |
| OBJECT | JSONB | Object → JSON |
| MAP | JSONB | Map → JSON |

### Geographic Type

| BigQuery Type | PostgreSQL Type | Notes |
|---------------|-----------------|-------|
| GEOGRAPHY, GEOG | GEOGRAPHY | Requires PostGIS |

### Interval Type

| BigQuery Type | PostgreSQL Type | Notes |
|---------------|-----------------|-------|
| INTERVAL, DURATION | INTERVAL | Time duration |

## Field Modes

BigQuery supports field modes that affect the PostgreSQL type:

### NULLABLE (Default)

```python
get_postgresql_type('INT64', 'NULLABLE')  # Returns 'BIGINT'
get_postgresql_type('STRING', 'NULLABLE') # Returns 'TEXT'
```

Field can be NULL. No NOT NULL constraint added.

### REQUIRED

```python
get_postgresql_type('INT64', 'REQUIRED')  # Returns 'BIGINT NOT NULL'
get_postgresql_type('STRING', 'REQUIRED') # Returns 'TEXT NOT NULL'
```

Field cannot be NULL. NOT NULL constraint added.

### REPEATED

```python
get_postgresql_type('INT64', 'REPEATED')    # Returns 'JSONB'
get_postgresql_type('STRING', 'REPEATED')   # Returns 'JSONB'
get_postgresql_type('RECORD', 'REPEATED')   # Returns 'JSONB'
```

Array field. Always converted to JSONB, regardless of base type.

## Type Categories

Types are grouped into categories for easier processing:

```python
from src.type_mappings import get_type_category

get_type_category('INT64')      # Returns 'NUMERIC'
get_type_category('STRING')     # Returns 'STRING'
get_type_category('STRUCT')     # Returns 'COMPLEX'
get_type_category('GEOGRAPHY')  # Returns 'GEOGRAPHIC'
```

### Categories

- **NUMERIC** - All integer and decimal types
- **STRING** - Text and character types
- **BINARY** - Binary data types
- **DATETIME** - Date, time, and timestamp types
- **BOOLEAN** - Boolean/bool types
- **COMPLEX** - STRUCT, ARRAY, JSON, OBJECT, MAP → all become JSONB
- **GEOGRAPHIC** - Geographic types (requires PostGIS)
- **INTERVAL** - Duration types
- **OTHER** - Unknown types (defaults to TEXT)

## Special Conversions

### REPEATED Fields (Arrays)

All repeated fields are converted to JSONB in PostgreSQL:

```python
# BigQuery schema
field = SchemaField("tags", "STRING", mode="REPEATED")

# Becomes
get_postgresql_type("STRING", "REPEATED")  # Returns 'JSONB'

# PostgreSQL table
CREATE TABLE ... (
    tags JSONB  -- Array of strings stored as JSON
)
```

### RECORD/STRUCT Fields (Nested Objects)

Nested objects are converted to JSONB:

```python
# BigQuery schema
field = SchemaField("metadata", "RECORD", fields=[
    SchemaField("key", "STRING"),
    SchemaField("value", "STRING")
])

# Becomes
get_postgresql_type("RECORD", "NULLABLE")  # Returns 'JSONB'

# PostgreSQL table
CREATE TABLE ... (
    metadata JSONB  -- Nested object stored as JSON
)
```

### Value Conversion

When transferring data, values are automatically converted:

```python
from src.type_mappings import needs_json_conversion

if needs_json_conversion("STRUCT"):
    # Convert Python dict to JSON string
    value = json.dumps(bq_value)
```

## Adding New Type Mappings

To add support for a new type:

1. **Edit `src/type_mappings.py`:**

```python
BQ_TO_PG_TYPE_MAPPING = {
    # ... existing mappings
    'NEWTYPE': 'POSTGRESQL_EQUIVALENT',
}

# Add to category
TYPE_CATEGORIES = {
    # ... existing categories
    'NEWCATEGORY': ['NEWTYPE', ...],
}
```

2. **Test the mapping:**

```python
from src.type_mappings import get_postgresql_type
assert get_postgresql_type('NEWTYPE') == 'POSTGRESQL_EQUIVALENT'
```

3. **Update this documentation:**

Add the type to the appropriate section in this file.

## Default Type

If a BigQuery type is not found in the mappings, it defaults to **TEXT**:

```python
get_postgresql_type('UNKNOWN_TYPE')  # Returns 'TEXT'
```

This is a safe default that works for most unrecognized types.

## JSON/JSONB Conversions

Complex types (RECORD, STRUCT, ARRAY, etc.) are stored as JSONB in PostgreSQL because:

1. **Flexible schema** - Nested structures vary by row
2. **Queryable** - JSONB supports indexing and queries
3. **Native support** - PostgreSQL has excellent JSONB performance
4. **Backward compatible** - Easy to convert to other formats later

Example:

```sql
-- BigQuery STRUCT
SELECT metadata.user_id, metadata.created_at FROM table

-- PostgreSQL JSONB equivalent
SELECT metadata->>'user_id', metadata->>'created_at' FROM table
SELECT * FROM table WHERE metadata->'user_id' = '123'
```

## PostGIS for Geography

For GEOGRAPHY types, PostgreSQL requires the PostGIS extension:

```sql
-- Enable PostGIS
CREATE EXTENSION postgis;

-- Then use GEOGRAPHY type
CREATE TABLE locations (
    id BIGINT,
    point GEOGRAPHY(POINT, 4326)
);
```

Ensure PostGIS is installed on your PostgreSQL server:

```bash
# Install PostGIS extension
sudo apt-get install postgresql-14-postgis

# Then in psql
CREATE EXTENSION postgis;
```

## References

- [BigQuery Data Types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
- [PostgreSQL Data Types](https://www.postgresql.org/docs/current/datatype.html)
- [PostgreSQL JSONB](https://www.postgresql.org/docs/current/datatype-json.html)
- [PostGIS](https://postgis.net/)
