# Partitioned Table Creation

## Supported Syntax

The OneLake extension supports creating partitioned Delta tables using a session variable:

```sql
SET onelake_partition_columns = 'category,region';
CREATE TABLE sales (
    id INTEGER,
    region VARCHAR,
    category VARCHAR,
    date DATE,
    amount DOUBLE
);
-- Partition setting is automatically consumed during CREATE TABLE
```

## Why Not PARTITION BY?

The standard SQL `PARTITION BY` clause in CREATE TABLE is **not natively supported** due to DuckDB's parser architecture. When you attempt to use it:

```sql
CREATE TABLE sales (...) PARTITION BY (category, region);
```

You'll receive a helpful error message with the correct syntax to use.

## Workaround for PARTITION BY Syntax

If you prefer to write queries with `PARTITION BY` syntax, you can:

### Option 1: Manual Preprocessing
Copy the suggested syntax from the error message:
```sql
-- Original (will error):
-- CREATE TABLE test (...) PARTITION BY (col1, col2);

-- Use instead:
SET onelake_partition_columns = 'col1,col2';
CREATE TABLE test (...);
```

### Option 2: Python Preprocessor
Use the provided script to automatically transform your SQL:

```bash
python scripts/preprocess_sql.py your_query.sql | duckdb
```

The script transforms:
```sql
CREATE TABLE test (id INT, category VARCHAR) PARTITION BY (category);
```

Into:
```sql
SET onelake_partition_columns = 'category';
CREATE TABLE test (id INT, category VARCHAR);
SET onelake_partition_columns = NULL;
```

## How It Works

1. Set the `onelake_partition_columns` variable before CREATE TABLE
2. The extension reads this variable during table creation
3. Partition columns are passed to the Delta Lake writer
4. Tables are created with Hive-style partitioning
5. The setting is automatically cleared after use

## Examples

### Single Partition Column
```sql
SET onelake_partition_columns = 'region';
CREATE TABLE users (
    id INTEGER,
    name VARCHAR,
    region VARCHAR
);
```

### Multiple Partition Columns
```sql
SET onelake_partition_columns = 'region,date';
CREATE TABLE events (
    id INTEGER,
    event_type VARCHAR,
    region VARCHAR,
    date DATE,
    value DOUBLE
);
```

### Verify Partitioning
After inserting data, partitions will be visible in OneLake/Fabric:
```
Tables/
  events/
    region=US/
      date=2024-01-01/
        part-00000.parquet
      date=2024-01-02/
        part-00000.parquet
    region=EU/
      date=2024-01-01/
        part-00000.parquet
```

## Limitations

- Partition columns must exist in the table schema
- Setting applies only to the next CREATE TABLE statement
- Static builds don't include the helpful PARTITION BY error message
- External query preprocessing required for true PARTITION BY syntax support
