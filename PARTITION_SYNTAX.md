# Partitioned Table Creation

## Supported Syntax

The OneLake extension now accepts native `PARTITION BY` clauses in `CREATE TABLE` statements and automatically
translates them into the underlying `onelake_partition_columns` session setting.

```sql
CREATE TABLE sales (
    id INTEGER,
    region VARCHAR,
    category VARCHAR,
    date DATE,
    amount DOUBLE
) PARTITION BY (category, region);
```

Behind the scenes the extension scopes `SET onelake_partition_columns = 'category,region';` just for the duration of the
statement, ensuring the Delta writer receives the same partition metadata without any manual steps.

## Manual Session Setting (Optional)

If you prefer the previous workflow, you can still set the session variable directly:

```sql
SET onelake_partition_columns = 'category,region';
CREATE TABLE sales (
    id INTEGER,
    region VARCHAR,
    category VARCHAR,
    date DATE,
    amount DOUBLE
);
SET onelake_partition_columns = NULL; -- optional cleanup
```

Both syntaxes feed into the same code path when the OneLake extension calls the Delta writer.

## How It Works

1. The parser extension intercepts `CREATE TABLE ... PARTITION BY (...)` statements.
2. It extracts the partition column list and rewrites the command internally.
3. `onelake_partition_columns` is set for the duration of the CREATE TABLE call.
4. The rewrite is transparent to users and preserves existing transaction semantics.

## Examples

### Single Partition Column
```sql
CREATE TABLE users (
    id INTEGER,
    name VARCHAR,
    region VARCHAR
) PARTITION BY (region);
```

### Multiple Partition Columns with Tags
```sql
CREATE TABLE events (
    id INTEGER,
    event_type VARCHAR,
    region VARCHAR,
    date DATE,
    value DOUBLE
) PARTITION BY (region, date)
WITH (description = 'Event stream for dashboards');
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

- Partition columns must exist in the table schema.
- The clause applies to the immediately following `CREATE TABLE` statement.
- Static builds without the parser extension fallback still require the manual session variable approach.
