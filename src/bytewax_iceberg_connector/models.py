"""Pydantic models for Iceberg connector configuration.

This module provides validated configuration models for Iceberg sinks,
ensuring type safety and providing excellent IDE support and documentation.
"""

from typing import Any, Dict, List, Optional
from enum import Enum
import pydantic
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import SortOrder as IcebergSortOrder, UNSORTED_SORT_ORDER


class IcebergFileFormat(Enum):
    """
    Iceberg file format options.
    
    Ref: https://iceberg.apache.org/spec/#file-formats
    """
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"


class IcebergSinkMessage(BaseModel):
    """Message to be written to Iceberg with full control.

    **What**: Represents a record to be written to an Iceberg table
    **Why**: Provides full control over record structure and metadata
    **When**: Used as output from your processing functions

    **Production Benefits**:
    - Complete control over record structure
    - Support for complex nested data types
    - Event time preservation for time-based processing
    - Partition key handling for performance optimization

    Attributes:
        data: Record data as a dictionary
        table: Target table (optional, can override sink table)
        partition_data: Optional partition override data
        event_timestamp: Event time (milliseconds)

    Example:
        ```python
        def create_iceberg_record(processed_data):
            return IcebergSinkMessage(
                data={
                    'user_id': processed_data['user_id'],
                    'event_type': processed_data['event_type'],
                    'timestamp': processed_data['timestamp'],
                    'properties': processed_data.get('properties', {})
                },
                event_timestamp=int(time.time() * 1000)
            )
        ```
    """

    data: Dict[str, Any]
    table: Optional[str] = None
    partition_data: Optional[Dict[str, Any]] = None
    event_timestamp: Optional[int] = None


class IcebergError(BaseModel):
    """Error from an Iceberg sink with context.

    **What**: Represents an error that occurred during record processing
    **Why**: Provides error context for debugging and monitoring
    **When**: Generated when record writing or processing fails

    **Production Benefits**:
    - Detailed error information for debugging
    - Original record context for error analysis
    - Enables dead letter patterns
    - Supports error monitoring and alerting

    Attributes:
        error: Human-readable error message
        record: Original record that caused the error

    Example:
        ```python
        def handle_errors(error: IcebergError):
            # Log error with context
            logger.error(f"Iceberg write failed: {error.error}")
            logger.error(f"Original record: {error.record.data}")

            # Send to monitoring system
            metrics.increment('iceberg.write.errors')
            return error.record  # Retry or send to DLQ
        ```
    """

    error: str
    """Error message."""

    record: IcebergSinkMessage
    """Record attached to that error."""


class IcebergSinkConfig(BaseModel):
    """
    Validated configuration for Iceberg sinks.
    
    This model provides type-safe configuration with sensible defaults
    optimized for high-throughput scenarios (millions of records/day).

    Example:
        ```python
        from bytewax_iceberg_connector import IcebergSinkConfig

        # High-throughput configuration
        config = IcebergSinkConfig(
            catalog_uri="thrift://localhost:9083",
            table_name="analytics.events",
            warehouse_path="s3://data-lake/warehouse",
            catalog_type="hive",
            batch_size=10000,
            batch_interval_seconds=30,
            num_partitions=8
        )

        sink = IcebergSink(config)
        ```
    """

    # Catalog configuration
    catalog_uri: str = Field(
        ..., 
        description="URI for the Iceberg catalog (e.g., thrift://host:port)"
    )
    table_name: str = Field(
        ..., 
        description="Fully qualified Iceberg table name (e.g., 'database.table')"
    )
    warehouse_path: str = Field(
        ..., 
        description="Path to the Iceberg warehouse (e.g., 's3://bucket/warehouse')"
    )
    catalog_type: str = Field(
        ..., 
        description="Catalog type for Iceberg (e.g., 'nessie', 'hive', 'rest')"
    )

    # Performance tuning
    batch_size: int = Field(
        1000, 
        ge=1, 
        le=100000, 
        description="Number of records per batch write to Iceberg"
    )
    batch_interval_seconds: int = Field(
        15,
        ge=1,
        le=300,
        description="Maximum seconds to wait before flushing a partial batch"
    )
    num_partitions: int = Field(
        1, 
        ge=1, 
        le=100, 
        description="Number of parallel sink partitions for writing"
    )

    # File configuration
    file_format: IcebergFileFormat = Field(
        IcebergFileFormat.PARQUET,
        description="File format for Iceberg data files"
    )
    temp_dir: str = Field(
        "/tmp", 
        description="Temporary directory for staging data files"
    )

    # Schema and partitioning (optional)
    table_schema: Optional[IcebergSchema] = Field(
        None, 
        description="Optional Iceberg schema for table creation"
    )
    partition_spec: Optional[IcebergPartitionSpec] = Field(
        None, 
        description="Optional Iceberg partition spec for table creation"
    )
    sort_order: Optional[IcebergSortOrder] = Field(
        None, 
        description="Optional Iceberg sort order for table creation"
    )
    location: Optional[str] = Field(
        None, 
        description="Optional table location override"
    )

    # Advanced options
    extra_options: Dict[str, Any] = Field(
        default_factory=dict, 
        description="Additional options for pyiceberg/pyarrow"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("batch_interval_seconds", mode="after")
    @classmethod
    def validate_batch_interval(cls, v, values):
        """Ensure batch interval makes sense for high throughput."""
        if v > 60:
            print(f"Warning: High batch interval ({v}s) may impact data freshness")
        return v

    @field_validator("num_partitions", mode="after")
    @classmethod
    def validate_num_partitions(cls, v, values):
        """Ensure partition count is reasonable."""
        if v > 50:
            print(f"Warning: High partition count ({v}) may impact coordination overhead")
        return v

    def for_high_throughput(self) -> "IcebergSinkConfig":
        """Return a copy optimized for high throughput scenarios.

        Returns:
            IcebergSinkConfig with settings optimized for millions of records/day
        """
        return self.model_copy(
            update={
                "batch_size": 10000,
                "batch_interval_seconds": 30,
                "num_partitions": 8,
                "file_format": IcebergFileFormat.PARQUET,
            }
        )

    def for_low_latency(self) -> "IcebergSinkConfig":
        """Return a copy optimized for low latency scenarios.

        Returns:
            IcebergSinkConfig with settings optimized for minimal latency
        """
        return self.model_copy(
            update={
                "batch_size": 100,
                "batch_interval_seconds": 5,
                "num_partitions": 2,
            }
        )

    def for_petabyte_scale(self) -> "IcebergSinkConfig":
        """Return a copy optimized for petabyte-scale processing.

        Returns:
            IcebergSinkConfig with settings optimized for massive scale
        """
        return self.model_copy(
            update={
                "batch_size": 50000,
                "batch_interval_seconds": 60,
                "num_partitions": 32,
                "file_format": IcebergFileFormat.PARQUET,
            }
        )


class IcebergSinkState(BaseModel):
    """
    Represents the checkpointed state required to safely resume or reprocess a failed 
    Iceberg sink partition in Bytewax.

    **What**: Maintains the state needed for exactly-once processing guarantees
    **Why**: Enables fault-tolerant processing with recovery capabilities
    **When**: Automatically managed by Bytewax for checkpointing and recovery

    Fields:
    - buffer: List of records (dicts) received but not yet committed to Iceberg
    - last_committed_batch: Identifiers of the most recently committed files
    - Additional metadata for robust recovery

    This state object is serialized and stored by Bytewax during checkpointing, 
    and is restored after failure or restart to guarantee correct, exactly-once 
    delivery and atomicity in the Iceberg sink.
    """
    buffer: List[Dict[str, Any]]
    last_committed_batch: Optional[Any] = None
    last_flush_time: Optional[float] = None 