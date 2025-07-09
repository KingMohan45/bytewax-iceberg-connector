# Implementing an Iceberg Sink in Bytewax

This guide explains how to build a robust, partitioned, and parallel Iceberg sink for Bytewax using the `pyiceberg` and `pyarrow` libraries. It covers the integration points, best practices, and how to leverage Bytewax’s abstractions for batch and streaming compatibility.

---

## 1. Overview
- **Iceberg** is a table format for large analytic datasets, supporting partitioned, atomic, and scalable data writes.
- **pyiceberg** provides Python APIs for writing files and committing them to Iceberg tables.
- **pyarrow** is used to efficiently write Parquet (or ORC) files for each partition.

---

## 1a. Iceberg Schema Construction and Usage

### Building Schemas for Iceberg Sinks

When writing to Iceberg tables using PyIceberg, you must define your table schema using the correct types and field objects from `pyiceberg.types`, **not** Pydantic or other libraries. For clarity and to avoid name collisions, always alias these imports with an `Iceberg` prefix:

```python
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    StringType as IcebergStringType,
    TimestampType as IcebergTimestampType,
    NestedField as IcebergNestedField,
)

# Example schema definition for a device uplink event table
streaming_uplink_test_schema = IcebergSchema(
    fields=[
        IcebergNestedField(id=1, name="device_id", required=False, type=IcebergStringType()),
        IcebergNestedField(id=2, name="organization_id", required=False, type=IcebergStringType()),
        IcebergNestedField(id=3, name="event_timestamp", required=False, type=IcebergTimestampType()),
        IcebergNestedField(id=4, name="event_type", required=False, type=IcebergStringType()),
        IcebergNestedField(id=5, name="payload", required=False, type=IcebergStringType()),
        IcebergNestedField(id=6, name="error", required=False, type=IcebergStringType()),
        IcebergNestedField(id=7, name="raw", required=False, type=IcebergStringType()),
    ]
)
```

**Best Practices:**
- Always use `IcebergNestedField` (aliased from `pyiceberg.types.NestedField`) for each column.
- Use type aliases (e.g., `IcebergStringType`) for all Iceberg types to avoid confusion with similar names from other libraries.
- Do **not** use Pydantic or other schema libraries for Iceberg table schemas.

### Using Schemas to Write to Iceberg Sinks

The schema object is used to:
- Validate and transform records before writing.
- Convert Python dictionaries to Arrow tables for Parquet writing:

```python
import pyarrow as pa
arrow_schema = streaming_uplink_test_schema.as_arrow()
arrow_table = pa.Table.from_pylist(records, schema=arrow_schema)
```

- Ensure compatibility with the Iceberg table when registering new files via PyIceberg's commit/append API.

**Integration:**
- Pass the schema to your sink or partition class as needed.
- Use the schema to validate records, transform types, and construct Arrow tables for efficient batch writes.
- Always keep your schema definition in sync with the actual Iceberg table metadata.

## 1b. Pitfalls and Gotchas

- Static schemas only; no schema evolution or inference.
- Schema mismatch (missing/extra fields or type mismatches) causes write failures.
- Partition spec misconfiguration leads to partitioning errors.
- Stale metadata errors can occur without reloading table metadata.
- Concurrent commit conflicts require retry on table.append to preserve exactly-once semantics.
- Arrow schema mismatches will trigger errors during conversion.

## 1c. Necessary Elements for a Working Iceberg Sink

- A pre-created Iceberg table with the correct static schema and partition spec.
- Valid `IcebergSinkConfig` with `catalog_uri`, `catalog_type`, `warehouse_path`, and `table_name`.
- Appropriate `batch_size` and `batch_interval_seconds` settings for your workload.
- Optional `arrow_schema` when strict typing is needed.
- Proper file format (`parquet`) and temp directory permissions.
- Unique partition identifiers (`part-0`, `part-1`, ...) matching `num_partitions`.
- Bytewax checkpoint integration via `snapshot` and `restore` methods.

---

## 2. Partitioned & Parallel Writes
- **Partitioning:** Iceberg tables are partitioned by one or more columns. Each Bytewax worker (or partition) should write data for a specific partition to its own file.
- **Parallelism:** You can write multiple partition files in parallel using threads or processes, as long as each file is independent.
- **Dynamic Partition Key Handling (2025-04):**
    - Partition key logic now dynamically maps integer ordinals from the Iceberg partition spec to field names using the table schema.
    - This fixes runtime errors when the partition spec uses integer ordinals (e.g., `[2, 3]`) instead of field names.
    - The code supports both named and ordinal partition specs, ensuring robust partitioning regardless of table creation method.
- **Integration Pattern:**
  1. Partition incoming records by the Iceberg partition spec (now supports both field names and ordinals).
  2. Buffer records for each partition.
  3. Periodically (or on checkpoint), flush each buffer to a Parquet file using `pyarrow`.
  4. After writing, register the new files with the Iceberg table using `pyiceberg` (but see commit coordination below).

---

## 3. Commit Coordination
- **Atomic Commits:** All new files for a batch should be committed together to the Iceberg table manifest. This ensures atomic visibility and avoids partial updates.
- **Concurrency:** Parallel file writes are safe, but avoid concurrent commits to the same table version. Use optimistic concurrency (Iceberg handles this) or coordinate commits at the end of each batch.
- **Exactly-once Semantics:** Track which files have been committed to avoid duplicates on retries. Use Bytewax’s checkpointing to persist commit state.

---

## 4. Integration Points with Bytewax
- **Sink Abstraction:** Use `FixedPartitionedSink` for robust, partitioned, and checkpointable output. Each partition can manage its own buffer and commit state.
- **Partition Class:**
  - Buffers records for a single partition.
  - Writes batches to Parquet files.
  - Registers files with Iceberg on checkpoint/commit.
  - Tracks last-committed batch/file for recovery.
  - **Partition Key Bug Fix (2025-04):** Partition key extraction now supports both integer and string field references, using a schema mapping to ensure correct key extraction.
- **Transform Logic:**
  - Transforms and flattens nested Kafka payloads (e.g., `UplinkEventMessage`) into the flat schema expected by the Iceberg table.
  - Handles timestamp conversion (e.g., ms since epoch to UTC datetime) and error propagation (error/raw columns).
- **Testing:**
  - Unit tests now cover both named and ordinal partition specs, all transformation edge cases, and proper error handling.
- **Linting & Robustness:**
  - Improved linting and exception chaining for production stability.
- **Checkpointing:** Use the `snapshot` method to persist commit state (e.g., last batch/file committed) for exactly-once delivery.
- **Batching:** Align Bytewax checkpoints with batch boundaries to ensure consistency between Bytewax state and Iceberg table state.

---

## 4a. Data Transformation Pipeline

To successfully write data to an Iceberg table using this connector, records must be transformed through several steps to ensure compatibility with both the table schema and the storage format:

1. **Application Object → Flat Dictionary**
   - Input data (e.g., Kafka messages or Pydantic models) are first transformed into flat Python dictionaries.
   - Each dictionary key must match a column in the Iceberg table schema. All required fields must be present; extra fields will be ignored or cause errors depending on the schema enforcement.
   - Data types must match the Iceberg schema:
     - Timestamps should be Python `datetime` objects for TIMESTAMP columns.
     - Strings, integers, and other primitives should be native Python types.
     - Nested or complex objects should be serialized (e.g., as JSON strings) if the table column is VARCHAR.

2. **Partition Key Extraction**
   - The connector uses the Iceberg table's partition spec to extract partition keys from each record.
   - **Partition transforms are applied dynamically:**
     - For derived partition fields (e.g., `event_timestamp_day` from `day(event_timestamp)`), the connector looks up the source field (here, `event_timestamp`) in the record and applies the transform (here, `day`).
     - The record does **not** need to contain a literal field for the derived partition (e.g., `event_timestamp_day`).
     - Only the source field (e.g., `event_timestamp`) must be present in the record, and must be of the correct type (e.g., Python `datetime`).
   - Partition key extraction supports both field names and integer ordinals, mapping them to dictionary keys using the table schema.
   - Partition transform functions (e.g., `day()`, `identity()`) are applied to the extracted values. The record must provide values of the correct type for these transforms (e.g., `day()` expects a datetime/timestamp).

### Requirements for Partition Transforms
- **Source Fields:** Your record must provide all source fields referenced by the partition spec (e.g., if partitioning by `day(event_timestamp)`, you must provide `event_timestamp`).
- **No Derived Fields Needed:** Do **not** add derived partition fields (e.g., `event_timestamp_day`) to your record; these are computed by the connector at runtime, following the Iceberg partition transform logic.
- **Correct Types:** Ensure the source fields are of the correct type for the transform (e.g., Python `datetime` for `day()` transforms). See the [Iceberg Python partitioning implementation](https://github.com/apache/iceberg-python/blob/main/pyiceberg/partitioning.py#L67) for details on supported transforms and type requirements.
- **Schema Alignment:** The partition spec in Iceberg metadata must match your intended partitioning logic. If you change partitioning, update the Iceberg table and restart the pipeline.
- **Reference:** For the full list of supported partition transforms and their semantics, see the [Iceberg specification on partition transforms](https://iceberg.apache.org/spec/#partition-transforms).

3. **Batching and Arrow Table Conversion**
   - Records are buffered in batches for efficient writing.
   - When a batch is ready, the list of dictionaries is converted to a `pyarrow.Table` using `pa.Table.from_pylist(records)`.
   - PyArrow validates types and raises errors if there are mismatches between the dicts and the expected schema.

4. **Parquet File Writing**
   - The Arrow Table is written to a Parquet file using `pyarrow.parquet.write_table`.
   - The Parquet file is then registered with the Iceberg table via PyIceberg's append/commit API.

**Best Practices:**
- Always ensure your data transformation step produces dicts with the correct field names and types.
- Validate that your partition spec matches your data types (e.g., do not apply `day()` to a string field).
- Use serialization (e.g., `json.dumps`) for any nested objects stored in VARCHAR columns.

---

## 5. Example Workflow
```python
# Pseudocode for a partition class
class IcebergSinkPartition(StatefulSinkPartition):
    def __init__(self, ...):
        self.buffer = []
        self.last_committed_batch = None

    def write_batch(self, items):
        self.buffer.extend(items)
        if len(self.buffer) >= BATCH_SIZE:
            self._flush_and_commit()

    def _flush_and_commit(self):
        # Write buffer to Parquet using pyarrow
        # Register file with Iceberg using pyiceberg
        # Update last_committed_batch
        self.buffer.clear()

    def snapshot(self):
        return {"last_committed_batch": self.last_committed_batch}
```

---

## 6. Best Practices
- Partition data before writing; write each partition to a separate file.
- Perform a single commit per batch for atomicity.
- Track committed files/batches for exactly-once semantics.
- Leverage Bytewax partitioning and checkpointing for parallelism and recovery.
- Document any requirements for idempotence or deduplication in your implementation.

---

## References
- [PyIceberg Documentation – Writing Data](https://py.iceberg.apache.org/latest/usage/writing/): Official Python API usage for writing to Iceberg tables.
- [PyArrow Documentation – Parquet Write](https://arrow.apache.org/docs/python/parquet.html#writing-parquet-files): Writing Parquet files from Arrow tables.
- [Iceberg Table Partitioning Concepts](https://iceberg.apache.org/docs/latest/partitioning/): Overview of partitioning in Iceberg.
- [Iceberg Partition Transforms (Spec)](https://iceberg.apache.org/spec/#partition-transforms): Full specification of supported partition transforms and semantics.
- [Iceberg Python Partitioning Implementation](https://github.com/apache/iceberg-python/blob/main/pyiceberg/partitioning.py#L67): Source code for partition transform logic in PyIceberg.
- [Bytewax Custom Sink API](https://docs.bytewax.io/stable/api/bytewax/bytewax.outputs.html): API documentation for implementing custom sinks in Bytewax.

---

If you need a full code example or have specific integration questions, let me know!
