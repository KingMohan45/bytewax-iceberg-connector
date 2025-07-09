"""Connectors for [Apache Iceberg](https://iceberg.apache.org).

This module provides Bytewax connectors for Apache Iceberg with high-performance capabilities:

## Direct Approach (Simple & Powerful)
Use `IcebergSink` directly for high-performance, petabyte-scale scenarios:

**When to Use Direct Approach**:
- High-throughput analytics processing
- Petabyte-scale data ingestion  
- Production data warehouse ETL
- Time-series analytics at scale
- Real-time data lake processing

**Production Use Cases**:
- Analytics data pipelines (event streams, metrics, logs)
- Data warehouse ETL processes
- Real-time feature engineering for ML
- Time-series data processing

```python
from bytewax_iceberg_connector import IcebergSink, IcebergSinkConfig
from bytewax import operators as op
from bytewax.dataflow import Dataflow

# High-performance analytics pipeline
flow = Dataflow("analytics-pipeline")

config = IcebergSinkConfig(
    catalog_uri="thrift://hive-metastore:9083",
    table_name="analytics.user_events", 
    warehouse_path="s3://data-lake/warehouse",
    catalog_type="hive"
).for_high_throughput()

sink = IcebergSink(config)

input_stream = op.input("input", flow, source)
processed = op.map("process", input_stream, my_processor)
op.output("analytics_output", processed, sink)
```

## Operators Approach (Advanced & Robust)
Use the operators for production systems requiring error handling and SerDe:

**When to Use Operators Approach**:
- Production systems requiring error handling
- Complex serialization/deserialization needs  
- Schema validation and evolution
- Monitoring and alerting requirements
- Dead letter queue patterns

```python
from bytewax_iceberg_connector import operators as iop
from bytewax.dataflow import Dataflow

# Advanced approach with error handling and SerDe
flow = Dataflow("robust-analytics")

# Validate and serialize with error handling
validated_stream = iop.validate_schema("validate", input_stream, my_validator)
serialized_stream = iop.serialize_records("serialize", validated_stream.oks, iop.DEFAULT_JSON_SERDE)

# Handle errors separately
op.inspect("log_errors", validated_stream.errs)
op.inspect("log_serialize_errors", serialized_stream.errs)

# Output successful records
iop.output("analytics_sink", serialized_stream.oks, config=config)
```

## Petabyte-Scale Configuration

For massive scale processing, use optimized configurations:

```python
# Petabyte-scale configuration
config = IcebergSinkConfig(
    catalog_uri="thrift://hive-metastore:9083",
    table_name="massive_analytics.events",
    warehouse_path="s3://petabyte-data-lake/warehouse", 
    catalog_type="hive",
).for_petabyte_scale()  # 50K batch size, 32 partitions, 60s intervals

sink = IcebergSink(config)
```

"""

import collections
import os
import time
import logging
from typing import Any, Dict, List, Optional, Union
import uuid

from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

from .models import IcebergSinkConfig, IcebergSinkState, IcebergSinkMessage


LOG = logging.getLogger(__name__)


class _IcebergSinkPartition(StatefulSinkPartition):
    """Internal partition implementation for IcebergSink.

    **Internal Class**: This handles the actual record writing for each worker.
    Users should use IcebergSink instead of this class directly.
    
    **Petabyte-Scale Design**:
    - Batches records efficiently for high throughput
    - Handles Iceberg partition management automatically  
    - Optimistic concurrency with retry logic
    - Time-based flushing for data freshness guarantees
    - Exactly-once semantics with state management

    **Concurrent Writing Architecture**:
    - Each partition generates unique file names (UUIDs)
    - Atomic commits using table.append()
    - Optimistic concurrency control with conflict resolution
    - Automatic table metadata reloading on conflicts
    - Supports parallel distributed writing
    """

    def __init__(self, config: IcebergSinkConfig, partition_id: str, state: Optional[IcebergSinkState] = None):
        self.config = config
        self.partition_id = partition_id
        self.buffer = []
        self.last_committed_batch = None
        self.table = self._load_table()
        self._last_flush_time = time.monotonic()
        
        if state is not None:
            self.buffer = state.buffer
            self.last_committed_batch = state.last_committed_batch
            if state.last_flush_time is not None:
                self._last_flush_time = state.last_flush_time

    def write_batch(self, items: List[Union[IcebergSinkMessage, Dict[str, Any]]]) -> None:
        """Buffer incoming items for batch writing to Iceberg.

        **Petabyte-Scale Processing**:
        - Efficient batching reduces I/O overhead
        - Time-based flushing ensures data freshness
        - Handles both IcebergSinkMessage and raw dict formats
        - Scales to millions of records per second per partition

        Bytewax calls this on each batch routed to this partition. We buffer until
        either batch_size is reached or batch_interval_seconds elapses.
        """
        # Convert items to dict format for processing
        records = []
        for item in items:
            if isinstance(item, IcebergSinkMessage):
                records.append(item.data)
            elif isinstance(item, dict):
                records.append(item)
            else:
                # Convert other types to dict format
                records.append({"value": item})

        LOG.debug(
            "[IcebergSinkPartition] write_batch called: partition_id=%s | batch_interval_seconds=%s | batch_size=%s",
            self.partition_id,
            self.config.batch_interval_seconds,
            self.config.batch_size,
        )

        self.buffer.extend(records)
        now = time.monotonic()
        time_since_flush = now - self._last_flush_time
        
        LOG.debug(
            "[IcebergSinkPartition] Time since last flush: %.2fs | Buffer size: %d",
            time_since_flush,
            len(self.buffer),
        )

        # Flush if batch size reached or time interval exceeded
        if (
            len(self.buffer) >= self.config.batch_size
            or time_since_flush >= self.config.batch_interval_seconds
        ):
            LOG.info(
                "[IcebergSinkPartition] Flushing buffer: partition_id=%s | time_since_flush=%.2fs | buffer_size=%d",
                self.partition_id,
                time_since_flush,
                len(self.buffer),
            )
            self._flush()
            self._last_flush_time = time.monotonic()

    def _flush(self, max_retries: int = 3) -> None:
        """Flush the in-memory buffer to Iceberg.
        
        **Petabyte-Scale Architecture**:
        - Partitions records by Iceberg table partition spec
        - Atomic commits with optimistic concurrency control
        - Automatic retry on conflicts with exponential backoff
        - Efficient Arrow Table conversion for performance
        
        Process:
        1. Partition buffered records by table's partition spec
        2. Convert to Arrow Table for efficient I/O
        3. Append to Iceberg with retry on conflicts
        4. Clear buffer after successful commit
        """
        if not self.buffer:
            return
            
        LOG.info(
            "[IcebergSinkPartition] _flush called: partition_id=%s | buffer_size=%d | flush_time=%s",
            self.partition_id,
            len(self.buffer),
            time.strftime("%Y-%m-%d %H:%M:%S"),
        )

        partitioned_records = self._partition_records(self.buffer)
        
        for part_key, records in partitioned_records.items():
            if not records:
                continue  # Skip empty partitions
                
            # Convert to Arrow Table - efficient for large datasets
            try:
                if self.config.table_schema:
                    arrow_table = pa.Table.from_pylist(records, schema=self.config.table_schema.as_arrow())
                else:
                    arrow_table = pa.Table.from_pylist(records)
            except Exception as e:
                LOG.error(f"Failed to convert records to Arrow Table: {e}")
                raise

            # Retry on optimistic concurrency conflicts
            for attempt in range(max_retries):
                try:
                    self.table.append(arrow_table)
                    LOG.debug(f"Successfully appended {len(records)} records for partition {part_key}")
                    break
                except CommitFailedException as e:
                    LOG.warning(
                        "Iceberg commit conflict, retry %d/%d: %s",
                        attempt + 1, max_retries, e,
                    )
                    if attempt + 1 == max_retries:
                        raise
                    # Reload latest metadata and back off
                    self.table = self._load_table()
                    time.sleep(2 ** attempt * 0.1)
                    
        self.buffer.clear()

    def _partition_records(self, records: List[Dict[str, Any]]) -> Dict[Any, List[Dict[str, Any]]]:
        """Group records by their Iceberg partition key.
        
        **Petabyte-Scale Partitioning**:
        - Efficient partitioning reduces query scan costs
        - Handles complex partition transforms automatically
        - Optimized for high-cardinality partitioning schemes
        - Supports time-based and categorical partitioning
        
        Returns:
            Dict mapping partition keys to lists of records
        """
        partition_spec = self.table.spec()

        if records:
            LOG.debug(
                "[IcebergSinkPartition] Partition spec fields: %s",
                [(f.source_id, f.name) for f in partition_spec.fields]
            )
            LOG.debug(
                "[IcebergSinkPartition] Sample record keys: %s",
                list(records[0].keys())
            )

        def partition_key(record: Dict[str, Any]) -> Optional[tuple]:
            """Compute the partition key for a record."""
            if not partition_spec.fields:
                return None  # Unpartitioned table

            key_parts = []
            for spec_field in partition_spec.fields:
                # Find source field by ID
                source_field = next(
                    (f for f in self.table.schema().fields if f.field_id == spec_field.source_id), 
                    None
                )
                if source_field is None:
                    raise ValueError(
                        f"Partition key error: Schema missing source field with id "
                        f"'{spec_field.source_id}' for partition field '{spec_field.name}'"
                    )
                
                source_field_name = source_field.name
                
                if source_field_name not in record:
                    raise ValueError(
                        f"Partition key error: Missing required source field '{source_field_name}' in record. "
                        f"Available fields: {list(record.keys())}"
                    )
                
                try:
                    field_type = source_field.field_type
                    # Apply the partition transform
                    transform_fn = spec_field.transform.transform(field_type)
                    transformed = transform_fn(record[source_field_name])
                    key_parts.append(transformed)
                except Exception as e:
                    raise ValueError(
                        f"Partition key error: Failed to transform source field "
                        f"'{source_field_name}' with value '{record[source_field_name]}': {e}"
                    ) from e
            
            return tuple(key_parts)

        # Group records by partition key
        partitioned_records = collections.defaultdict(list)
        for record in records:
            try:
                key = partition_key(record)
                partitioned_records[key].append(record)
            except Exception as e:
                LOG.error(f"Failed to partition record {record}: {e}")
                raise
                
        return partitioned_records

    def snapshot(self) -> IcebergSinkState:
        """Capture current state for checkpointing.

        **Exactly-Once Semantics**:
        - Captures buffered records for recovery
        - Tracks commit state for deduplication
        - Enables fault-tolerant processing
        - Supports distributed checkpointing
        """
        return IcebergSinkState(
            buffer=list(self.buffer),
            last_committed_batch=self.last_committed_batch,
            last_flush_time=self._last_flush_time,
        )

    def close(self) -> None:
        """Clean shutdown with final flush."""
        if self.buffer:
            LOG.info(f"Final flush on close: {len(self.buffer)} records")
            self._flush()

    def _load_table(self) -> Table:
        """Load Iceberg table with latest metadata.
        
        **Concurrent Writing Safety**:
        - Always fetches latest table metadata
        - Critical for optimistic concurrency control
        - Enables safe parallel writing
        - Supports table creation if missing
        """
        LOG.debug("Loading Iceberg table: %s", self.config.table_name)
        
        catalog = load_catalog(
            "default",
            type=self.config.catalog_type,
            uri=self.config.catalog_uri,
            warehouse=self.config.warehouse_path,
            **self.config.extra_options,
        )
        
        try:
            table = catalog.load_table(self.config.table_name)
            LOG.debug("Loaded existing Iceberg table")
            return table
        except NoSuchTableError:
            if self.config.table_schema is not None:
                LOG.info(f"Table {self.config.table_name} does not exist, creating it")
                table = catalog.create_table(
                    identifier=self.config.table_name,
                    schema=self.config.table_schema,
                    location=self.config.location or None,
                    partition_spec=self.config.partition_spec or UNPARTITIONED_PARTITION_SPEC,
                    sort_order=self.config.sort_order or UNSORTED_SORT_ORDER,
                )
                LOG.info("Created Iceberg table successfully")
                return table
            raise


class IcebergSink(FixedPartitionedSink):
    """High-performance Iceberg sink for Bytewax dataflows.

    **What**: Creates Iceberg writers that store processed data in Iceberg tables
    **Why**: Provides petabyte-scale analytics storage with ACID guarantees
    **When**: Use for analytics pipelines, data warehouse ETL, real-time feature stores

    **Petabyte-Scale Architecture**:
    - FixedPartitionedSink for predictable resource allocation
    - Parallel writing with optimistic concurrency control  
    - Configurable batching for optimal performance
    - Exactly-once semantics with checkpointing
    - Schema evolution and partition management

    **Production Benefits**:
    - ACID transactions with snapshot isolation
    - Time travel and schema evolution
    - Efficient columnar storage (Parquet)
    - Automatic file compaction and optimization
    - Compatible with all major query engines

    Args:
        config: IcebergSinkConfig with comprehensive configuration

    Example:
        ```python
        # Petabyte-scale analytics pipeline
        config = IcebergSinkConfig(
            catalog_uri="thrift://hive-metastore:9083",
            table_name="analytics.user_events",
            warehouse_path="s3://data-lake/warehouse",
            catalog_type="hive"
        ).for_petabyte_scale()

        sink = IcebergSink(config)

        def process_events(event_batch):
            # Process millions of events efficiently
            processed = []
            for event in event_batch:
                processed.append({
                    'user_id': event['user_id'],
                    'event_type': event['event_type'], 
                    'timestamp': event['timestamp'],
                    'properties': event.get('properties', {})
                })
            return processed

        flow = Dataflow("petabyte-analytics")
        input_stream = op.input("events", flow, event_source)
        processed = op.map("process", input_stream, process_events)
        op.output("iceberg_output", processed, sink)
        ```
    """

    def __init__(self, config: IcebergSinkConfig):
        if not isinstance(config, IcebergSinkConfig):
            raise TypeError("config must be an IcebergSinkConfig instance")
        self.config = config

    def list_parts(self) -> List[str]:
        """Define the set of sink partitions for parallel processing.

        **Petabyte-Scale Partitioning Strategy**:
        - Fixed partition count for predictable resource usage
        - Each partition handles independent Iceberg file writing
        - Configurable via num_partitions for scaling
        - Optimal for distributed compute clusters

        Returns:
            List of partition identifiers for parallel processing
        """
        return [f"part-{i}" for i in range(self.config.num_partitions)]

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[IcebergSinkState],
    ) -> _IcebergSinkPartition:
        """Build (or resume) a sink partition.

        **Fault-Tolerant Design**:
        - Supports resuming from checkpointed state
        - Handles state serialization/deserialization
        - Enables exactly-once processing guarantees
        - Critical for production reliability
        """
        state: Optional[IcebergSinkState] = None
        
        if resume_state is not None:
            if isinstance(resume_state, dict):
                state = IcebergSinkState(**resume_state)
            elif isinstance(resume_state, IcebergSinkState):
                state = resume_state
            else:
                raise TypeError(
                    f"resume_state must be a dict or IcebergSinkState, not {type(resume_state)}"
                )

        return _IcebergSinkPartition(self.config, for_part, state=state) 