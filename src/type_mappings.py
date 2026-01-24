# Comprehensive BigQuery to PostgreSQL type mappings
BQ_TO_PG_TYPE_MAPPING = {
    # Numeric types - Integers
    'INT': 'INTEGER',
    'INT64': 'BIGINT',
    'INTEGER': 'BIGINT',
    'TINYINT': 'SMALLINT',
    'BYTEINT': 'SMALLINT',
    'SMALLINT': 'SMALLINT',
    'BIGINT': 'BIGINT',
    
    # Numeric types - Floating point
    'FLOAT': 'DOUBLE PRECISION',
    'FLOAT32': 'REAL',
    'FLOAT64': 'DOUBLE PRECISION',
    'DOUBLE': 'DOUBLE PRECISION',
    
    # Numeric types - Decimal/Precise
    'NUMERIC': 'NUMERIC',
    'DECIMAL': 'NUMERIC',
    'BIGNUMERIC': 'NUMERIC',
    'BIGDECIMAL': 'NUMERIC',
    
    # String types
    'STRING': 'TEXT',
    'VARCHAR': 'VARCHAR(255)',
    'CHAR': 'CHAR',
    'TEXT': 'TEXT',
    
    # Binary types
    'BYTES': 'BYTEA',
    'BINARY': 'BYTEA',
    'VARBINARY': 'BYTEA',
    
    # Boolean
    'BOOL': 'BOOLEAN',
    'BOOLEAN': 'BOOLEAN',
    
    # Date and time types
    'DATE': 'DATE',
    'TIME': 'TIME',
    'DATETIME': 'TIMESTAMP',
    'TIMESTAMP': 'TIMESTAMP WITH TIME ZONE',
    'TIMESTAMPTZ': 'TIMESTAMP WITH TIME ZONE',
    
    # Complex types - converted to JSON/JSONB
    'RECORD': 'JSONB',
    'STRUCT': 'JSONB',
    'ARRAY': 'JSONB',
    'JSON': 'JSONB',
    'OBJECT': 'JSONB',
    'MAP': 'JSONB',
    
    # Geographic types
    'GEOGRAPHY': 'GEOGRAPHY',
    'GEOG': 'GEOGRAPHY',
    
    # Interval/Duration
    'INTERVAL': 'INTERVAL',
    'DURATION': 'INTERVAL',
}

# Field modes in BigQuery
FIELD_MODES = {
    'NULLABLE': '',           # Optional field
    'REQUIRED': 'NOT NULL',   # Required field
    'REPEATED': 'JSONB',      # Array field - converted to JSONB
}

# Type categories for specialized handling
TYPE_CATEGORIES = {
    'NUMERIC': ['INT', 'INT64', 'INTEGER', 'TINYINT', 'BYTEINT', 'SMALLINT', 'BIGINT',
                'FLOAT', 'FLOAT32', 'FLOAT64', 'DOUBLE', 'NUMERIC', 'DECIMAL', 
                'BIGNUMERIC', 'BIGDECIMAL'],
    'STRING': ['STRING', 'VARCHAR', 'CHAR', 'TEXT'],
    'BINARY': ['BYTES', 'BINARY', 'VARBINARY'],
    'DATETIME': ['DATE', 'TIME', 'DATETIME', 'TIMESTAMP', 'TIMESTAMPTZ'],
    'COMPLEX': ['RECORD', 'STRUCT', 'ARRAY', 'JSON', 'OBJECT', 'MAP'],
    'GEOGRAPHIC': ['GEOGRAPHY', 'GEOG'],
    'BOOLEAN': ['BOOL', 'BOOLEAN'],
    'INTERVAL': ['INTERVAL', 'DURATION'],
}

# Reverse mapping: type -> category (optimized for faster lookups)
_TYPE_TO_CATEGORY_CACHE = {}
for category, types in TYPE_CATEGORIES.items():
    for bq_type in types:
        _TYPE_TO_CATEGORY_CACHE[bq_type] = category

# Types that need JSON conversion
_JSON_CONVERSION_TYPES = set(TYPE_CATEGORIES['COMPLEX'] + ['GEOGRAPHY', 'GEOG'])
_BINARY_TYPES = set(TYPE_CATEGORIES['BINARY'])


def get_postgresql_type(bq_type: str, mode: str = 'NULLABLE') -> str:

    # Handle REPEATED mode - always becomes JSONB (fast path)
    if mode == 'REPEATED':
        return 'JSONB'
    
    # Normalize type once (uppercase)
    bq_type_upper = bq_type.upper() if isinstance(bq_type, str) else 'TEXT'
    
    # Get PostgreSQL type (default to TEXT if unknown)
    pg_type = BQ_TO_PG_TYPE_MAPPING.get(bq_type_upper, 'TEXT')
    
    # Add NOT NULL constraint for REQUIRED fields (fast path)
    if mode == 'REQUIRED':
        return f"{pg_type} NOT NULL"
    
    return pg_type


def get_type_category(bq_type: str) -> str:
    bq_type_upper = bq_type.upper() if isinstance(bq_type, str) else 'OTHER'
    return _TYPE_TO_CATEGORY_CACHE.get(bq_type_upper, 'OTHER')


def needs_json_conversion(bq_type: str, mode: str = 'NULLABLE') -> bool:

    # REPEATED always needs JSON (fast path)
    if mode == 'REPEATED':
        return True
    
    # Use cached set for O(1) lookup
    bq_type_upper = bq_type.upper() if isinstance(bq_type, str) else ''
    return bq_type_upper in _JSON_CONVERSION_TYPES


def is_binary_type(bq_type: str) -> bool:

    bq_type_upper = bq_type.upper() if isinstance(bq_type, str) else ''
    return bq_type_upper in _BINARY_TYPES


def get_all_supported_types() -> dict:

    return BQ_TO_PG_TYPE_MAPPING.copy()


# Export main function for backward compatibility
__all__ = [
    'BQ_TO_PG_TYPE_MAPPING',
    'FIELD_MODES',
    'TYPE_CATEGORIES',
    'get_postgresql_type',
    'get_type_category',
    'needs_json_conversion',
    'is_binary_type',
    'get_all_supported_types',
]
