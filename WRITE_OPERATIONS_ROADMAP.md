# DuckDB OneLake Extension - Write Operations Implementation Roadmap

**Status:** In Progress
**Target Timeline:** 16-20 weeks for full implementation  
**Current Branch:** achrafcei/issue14

---

## Executive Summary

This roadmap outlines the incremental implementation of full DML (Data Manipulation Language) support for the DuckDB OneLake extension. Currently, only append-only INSERT operations are supported. This plan breaks down the work into 8 phases, each delivering testable functionality.

**Project Status:** Phases 0-4 Complete (November 28, 2025)

**Current State:**
- ✅ Append-only INSERT with Rust Delta writer
- ✅ **NEW: Configurable write modes (append/overwrite/error_if_exists/ignore)**
- ✅ **NEW: Schema evolution support (merge/overwrite)**
- ✅ **NEW: Performance tuning options (file size, batch size)**
- ✅ Catalog-level CREATE TABLE (no physical table creation yet)
- ✅ Read operations (Delta and Iceberg)
- ✅ Comprehensive test infrastructure
- ✅ Enhanced logging for write operations
- ✅ Build system fixes (vcpkg overlay, API compatibility)
- ✅ Physical table creation in storage (Phase 2)
- ✅ DROP TABLE support with safety checks (Phase 3)
- ✅ DELETE operations (Phase 4)
- ❌ UPDATE, MERGE, ALTER operations (Phases 5-7)

**Recent Build Fixes:**
- Fixed vcpkg `azure-core-cpp` overlay (removed MinGW-specific patches)
- Updated API calls for DuckDB compatibility (`TryGetCurrentSetting`, `GetAccessToken`)
- ✅ Clean build verified on Linux x64

**Progress Summary:**
- **Phase 0:** ✅ Complete - Foundation & Testing Infrastructure
- **Phase 1:** ✅ Complete - Expose Existing Write Modes
- **Phase 2:** ✅ Complete - Physical Table Creation
- **Phase 3:** ✅ Complete - DROP TABLE Support
- **Phase 4:** ✅ Complete - DELETE Operations
- **Phase 5:** ✅ Complete - UPDATE Operations
- **Next Up:** Phase 6 (MERGE), Phase 7 (ALTER), Phase 8 (GA hardening)

**Next Phases:**
1. **Phase 6 – MERGE Operations:** introduce source-stream-driven UPSERT/DELETE with new FFI contract.
2. **Phase 7 – ALTER TABLE:** add schema evolution primitives (ADD/DROP COLUMN) and catalog refresh logic.
3. **Phase 8 – Hardening & GA:** perf tuning, resilience, documentation polish, and full SQLLogicTest coverage.

---

## Completed Phases (0-5)

| Phase | Focus | Completion Date | Highlights |
|-------|-------|-----------------|------------|
| 0 | Foundation & test harness | 26 Nov 2025 | Created `test/sql/onelake_writes.test`, logging macros, and write API docs. |
| 1 | Write-mode surfacing | 26 Nov 2025 | Surfaced insert/schema modes, safe-cast, and performance knobs via SQL settings with dedicated tests. |
| 2 | Physical CREATE TABLE | 27 Nov 2025 | DuckDB `CREATE TABLE` now materializes Delta metadata, partition lists, and table properties inside OneLake. |
| 3 | DROP TABLE | 27 Nov 2025 | Added Rust drop FFI, Unity Catalog cleanup, and `onelake_allow_destructive_operations` gate. |
| 4 | DELETE | 28 Nov 2025 | Implemented predicate deletes with row-count telemetry and shared destructive-operation guard. |
| 5 | UPDATE | 28 Nov 2025 | Added `PhysicalOneLakeUpdate`, JSON assignment payloads, and regression suites for multi-column updates. |

**Key Takeaways:**
- All prerequisite catalog, credential, and writer surfaces are in place; no additional scaffolding is needed for upcoming work.
- The destructive-operation guard proved effective and will continue to protect MERGE/ALTER flows.
- Remaining roadmap items can focus on new functionality (MERGE, ALTER TABLE, GA polish) without revisiting earlier phases.

---

## Phase 6: MERGE Operations (Week 15-17)

**Goal:** Support MERGE (UPSERT) for Delta tables

### Tasks

#### 6.1: Add Merge FFI Function
**File:** `rust/onelake_delta_writer/src/lib.rs`

```rust
/// Performs a MERGE (upsert) operation on a Delta table.
#[no_mangle]
pub extern "C" fn ol_delta_merge(
    stream: *mut FFI_ArrowArrayStream,
    table_uri: *const c_char,
    merge_spec_json: *const c_char,
    token_json: *const c_char,
    options_json: *const c_char,
    error_buffer: *mut c_char,
    error_buffer_len: usize,
) -> c_int {
    let result = (|| -> Result<MergeMetrics, DeltaFfiError> {
        let table_uri = read_cstr(table_uri)?;
        let merge_spec_json = read_cstr(merge_spec_json)?;
        let token_json = read_cstr(token_json)?;
        let options_json = read_cstr(options_json)?;
        
        let batches = unsafe { collect_batches(stream)? };
        
        merge_impl(&table_uri, batches, &merge_spec_json, &token_json, &options_json)
    })();
    
    match result {
        Ok(metrics) => OlDeltaStatus::Ok as c_int,
        Err(err) => {
            write_error_message(error_buffer, error_buffer_len, &err.message());
            err.status() as c_int
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MergeSpec {
    join_condition: String,
    when_matched_update: Option<HashMap<String, String>>,
    when_matched_delete: Option<String>,
    when_not_matched_insert: Option<Vec<String>>,
}

struct MergeMetrics {
    rows_inserted: usize,
    rows_updated: usize,
    rows_deleted: usize,
}

fn merge_impl(
    table_uri: &str,
    source_batches: Vec<RecordBatch>,
    merge_spec_json: &str,
    token_json: &str,
    options_json: &str,
) -> Result<MergeMetrics, DeltaFfiError> {
    let spec: MergeSpec = serde_json::from_str(merge_spec_json)?;
    let tokens: TokenPayload = parse_json_or_default(token_json)?;
    let storage_options = merge_storage_options(HashMap::new(), &tokens);
    
    ensure_storage_handlers_registered();
    
    runtime()?.block_on(async move {
        let ops = if storage_options.is_empty() {
            DeltaOps::try_from_uri(&table_uri).await?
        } else {
            DeltaOps::try_from_uri_with_storage_options(&table_uri, storage_options).await?
        };
        
        let mut builder = ops.merge(source_batches, &spec.join_condition);
        
        if let Some(update_map) = spec.when_matched_update {
            builder = builder.when_matched_update(|mut update| {
                for (col, expr) in update_map {
                    update = update.update(&col, &expr);
                }
                update
            })?;
        }
        
        if let Some(delete_cond) = spec.when_matched_delete {
            builder = builder.when_matched_delete(|del| del.predicate(&delete_cond))?;
        }
        
        if let Some(insert_cols) = spec.when_not_matched_insert {
            builder = builder.when_not_matched_insert(|mut insert| {
                for col in insert_cols {
                    insert = insert.set(&col, &col); // Map source to target
                }
                insert
            })?;
        }
        
        let result = builder.await?;
        
        Ok(MergeMetrics {
            rows_inserted: result.num_inserted_rows,
            rows_updated: result.num_updated_rows,
            rows_deleted: result.num_deleted_rows,
        })
    })
}
```

#### 6.2: Implement MERGE Planning
**File:** `src/storage/onelake_merge.cpp`

Complex operator that:
- Accepts source data (from SELECT or VALUES)
- Defines join condition
- Handles WHEN MATCHED / WHEN NOT MATCHED clauses
- Coordinates Delta merge operation

#### 6.3: Test MERGE Operations
**File:** `test/sql/onelake_merge.test`

```sql
# Setup target table
statement ok
CREATE TABLE test_db.target (id INT PRIMARY KEY, name VARCHAR, value INT);

statement ok
INSERT INTO test_db.target VALUES (1, 'A', 100), (2, 'B', 200);

# Setup source table
statement ok
CREATE TABLE test_db.source (id INT, name VARCHAR, value INT);

statement ok
INSERT INTO test_db.source VALUES (2, 'B_updated', 250), (3, 'C', 300);

# MERGE operation
statement ok
MERGE INTO test_db.target t
USING test_db.source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value);

query IVI
SELECT * FROM test_db.target ORDER BY id;
----
1	A	100
2	B_updated	250
3	C	300
```

**Deliverables:**
- MERGE with INSERT and UPDATE
- Conditional MERGE (with additional WHERE)
- DELETE in MERGE
- Complex join conditions

**Estimated Effort:** 3-4 weeks  
**Risk:** Very High (most complex DML, requires deep integration)  
**Dependencies:** Phase 4, 5

---

## Phase 7: ALTER TABLE Support (Week 18-19)

**Goal:** Enable schema modifications

### Tasks

#### 7.1: Add Column
```sql
ALTER TABLE test_db.my_table ADD COLUMN new_col VARCHAR;
```

Implementation:
- Read current Delta metadata
- Append column to schema
- Write new metadata version
- Update catalog entry

#### 7.2: Drop Column
```sql
ALTER TABLE test_db.my_table DROP COLUMN old_col;
```

Implementation:
- Mark column as deleted in metadata
- Maintain backward compatibility (old files still have column)
- Update catalog

#### 7.3: Rename Column
```sql
ALTER TABLE test_db.my_table RENAME COLUMN old_name TO new_name;
```

Implementation:
- Update column name in metadata
- Optionally maintain alias for old name

#### 7.4: Change Column Type
```sql
ALTER TABLE test_db.my_table ALTER COLUMN col TYPE BIGINT;
```

Implementation:
- Validate type compatibility
- May require data rewrite
- Update metadata

**Deliverables:**
- ADD COLUMN support
- DROP COLUMN support
- RENAME COLUMN support
- ALTER COLUMN TYPE (limited)
- Protocol version management

**Estimated Effort:** 1-2 weeks  
**Risk:** Medium (metadata consistency, backward compatibility)  
**Dependencies:** Phase 2

---

## Phase 8: Advanced Features & Optimization (Week 20+)

**Goal:** Performance, Iceberg writes, and production hardening

### Tasks

#### 8.1: Iceberg Write Support
- Investigate Iceberg Rust libraries
- Implement similar FFI pattern
- Add Iceberg-specific operators

#### 8.2: Performance Optimization
- Connection pooling for Fabric API
- Metadata caching with TTL
- Parallel write operations
- Predicate pushdown for UPDATE/DELETE

#### 8.3: Transaction Management
- Optimistic concurrency control
- Conflict detection and retry
- Transaction isolation levels
- Snapshot isolation

#### 8.4: Error Recovery
- Failed write cleanup
- Partial transaction rollback
- Orphaned file cleanup
- Log compaction

#### 8.5: Advanced SQL Features
- INSERT ... RETURNING
- INSERT ... ON CONFLICT (upsert)
- TRUNCATE TABLE
- COPY FROM/TO OneLake tables

**Estimated Effort:** 4+ weeks  
**Risk:** Medium-High  
**Dependencies:** All previous phases

---

## Testing Strategy

### Unit Tests
- Rust: `cargo test` in `rust/onelake_delta_writer`
- C++: Extend `build/release/test/unittest`

### Integration Tests
- SQLLogicTest in `test/sql/onelake_*.test`
- Live OneLake connection required
- Environment variable configuration

### Stress Tests
- Concurrent write operations
- Large dataset inserts (millions of rows)
- Schema evolution edge cases
- Transaction conflict scenarios

### Regression Tests
- Ensure read operations still work
- Backward compatibility with existing tables
- Credential handling unchanged

---

## Risk Mitigation

### High-Risk Areas

1. **Data Loss in DROP TABLE**
   - Mitigation: Require explicit setting, add dry-run mode, backup recommendations

2. **Concurrent Write Conflicts**
   - Mitigation: Implement retry logic, transaction isolation, conflict detection

3. **Schema Incompatibility**
   - Mitigation: Strict validation, protocol version checks, rollback support

4. **Performance Degradation**
   - Mitigation: Benchmarking, profiling, optimization passes

### Rollback Strategy

Each phase is independently testable. If a phase fails:
1. Revert code changes for that phase
2. Previous phases remain functional
3. Document limitations in README
4. Schedule rework in next iteration

---

## Documentation Requirements

### Per-Phase Documentation

1. **User Guide Updates**
   - New SQL syntax examples
   - Configuration options
   - Best practices

2. **API Documentation**
   - FFI function signatures
   - Error codes
   - Return values

3. **Architecture Diagrams**
   - Updated flow charts
   - Sequence diagrams
   - Component interactions

### Final Documentation Deliverables

- Updated `README.md` with full DML examples
- Complete `DOCUMENTATION.md` with write operations
- `WRITE_OPERATIONS.md` technical reference
- Migration guide for users

---

## Success Criteria

### Phase Completion Checklist

Each phase is complete when:
- [ ] All code changes committed and reviewed
- [ ] Unit tests pass (>80% coverage)
- [ ] Integration tests pass
- [ ] Documentation updated
- [ ] Performance benchmarks acceptable
- [ ] Manual testing completed
- [ ] Code review approved

### Overall Project Success

Project is complete when:
- [ ] All 8 phases delivered
- [ ] Full DML support (INSERT, UPDATE, DELETE, MERGE)
- [ ] Table lifecycle (CREATE, ALTER, DROP)
- [ ] Production-ready error handling
- [ ] Comprehensive test coverage (>85%)
- [ ] Complete user documentation
- [ ] Performance meets benchmarks
- [ ] Security review passed

---

## Timeline Summary

| Phase | Description | Duration | Risk | Start Week |
|-------|-------------|----------|------|------------|
| 0 | Foundation & Testing | 1-2 weeks | Low | Week 1 |
| 1 | Expose Write Modes | 1-2 weeks | Low | Week 3 |
| 2 | Physical Table Creation | 2-3 weeks | Medium | Week 5 |
| 3 | DROP TABLE | 1 week | Medium | Week 8 |
| 4 | DELETE Operations | 2-3 weeks | High | Week 9 |
| 5 | UPDATE Operations | 2-3 weeks | High | Week 12 |
| 6 | MERGE Operations | 3-4 weeks | Very High | Week 15 |
| 7 | ALTER TABLE | 1-2 weeks | Medium | Week 18 |
| 8 | Advanced Features | 4+ weeks | Medium | Week 20+ |

**Total Estimated Time:** 16-20 weeks for Phases 0-7  
**Extended Timeline:** 24+ weeks including Phase 8

---

## Next Steps

1. **Immediate Actions (This Week)**
   - Review and approve this roadmap
   - Set up test environment with live OneLake access
   - Create branch structure for phases
   - Configure CI/CD for automated testing

2. **Phase 0 Kickoff (Next Week)**
   - Create test suite skeleton
   - Set up logging infrastructure
   - Document baseline performance metrics
   - Begin foundation work

3. **Regular Checkpoints**
   - Weekly progress reviews
   - Bi-weekly demos of completed features
   - Monthly architecture reviews
   - Quarterly planning adjustments

---

## Resources & Dependencies

### Required Resources
- Development environment with Rust and C++ toolchains
- OneLake test workspace with appropriate permissions
- CI/CD system for automated testing
- Code review capacity

### External Dependencies
- `deltalake` Rust crate updates
- DuckDB core API stability
- Microsoft Fabric API availability
- Azure authentication services

### Team Requirements
- C++ developer (catalog integration)
- Rust developer (FFI and Delta operations)
- DevOps engineer (CI/CD, testing)
- Technical writer (documentation)

---

## Appendix: Code Examples

### Example: New Write Mode Usage

```sql
-- Set session-level write mode
SET onelake_insert_mode = 'overwrite';

-- Or use table-level hint
INSERT INTO my_table /*+ MODE('append') */ 
SELECT * FROM source_table;

-- Schema evolution
SET onelake_schema_mode = 'merge';
INSERT INTO my_table (id, name, new_column) 
VALUES (1, 'Test', 'Value');
```

### Example: Full CRUD Operations

```sql
-- Create
CREATE TABLE sales (
    id INT,
    region VARCHAR,
    amount DECIMAL(10,2),
    date DATE
) WITH (partition_columns = ['region', 'date']);

-- Insert
INSERT INTO sales VALUES 
    (1, 'US', 100.00, '2025-01-01'),
    (2, 'EU', 150.00, '2025-01-01');

-- Update
UPDATE sales 
SET amount = amount * 1.1 
WHERE region = 'US';

-- Delete
DELETE FROM sales 
WHERE date < '2025-01-01';

-- Merge
MERGE INTO sales t
USING daily_sales s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET amount = s.amount
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.region, s.amount, s.date);

-- Drop
DROP TABLE sales;
```

---

**Document Version:** 1.0  
**Last Updated:** November 26, 2025  
**Author:** DuckDB OneLake Team  
**Status:** Draft - Awaiting Approval
