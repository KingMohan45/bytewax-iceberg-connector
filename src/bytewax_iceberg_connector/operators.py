"""Operators for the Iceberg sink.

This module provides advanced operators for Iceberg integration with error handling and SerDe:

## Use Cases

### When to Use Operators Approach
- Production systems requiring error handling
- Complex serialization/deserialization needs
- When you need error streams for monitoring/alerting
- Petabyte-scale data processing with reliability

### When to Use Direct Approach  
- Simple record processing without complex error handling
- Prototyping and development
- When you handle serialization manually

## Examples

### Basic Usage
```python
from bytewax.dataflow import Dataflow
from bytewax_iceberg_connector import operators as iop

flow = Dataflow("iceberg-processing")

# Process with error handling
processed_data = my_processing_stream
iceberg_messages = iop.serialize_records("serialize", processed_data, iop.DEFAULT_JSON_SERDE)
iop.output("iceberg_sink", iceberg_messages.oks, config=my_config)

# Handle errors
op.inspect("log_errors", iceberg_messages.errs)
```

### With Schema Evolution
```python
# Handle schema evolution and validation
validated_stream = iop.validate_schema("validate", input_stream, my_schema)
processed = op.map("process", validated_stream.oks, my_processor)
iop.output("sink", processed, config=config)
```
"""

import json
import pickle
import time
from typing import Any, Dict, List, Optional, Union, cast

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream, operator
import pyarrow as pa
from pydantic import BaseModel

from .models import IcebergError, IcebergSinkMessage, IcebergSinkConfig


class IcebergOpOut(BaseModel):
    """Result streams from Iceberg operators.

    This class encapsulates the two output streams from Iceberg operators:
    - `oks`: Successfully processed records
    - `errs`: Error records that failed processing

    **Production Use Case**: Allows you to handle errors separately from successful
    records, enabling robust error handling, monitoring, and dead letter processing.

    Example:
        ```python
        iceberg_output = iop.serialize_records("serialize", input_stream, iop.DEFAULT_JSON_SERDE)

        # Handle successful records
        iop.output("sink", iceberg_output.oks, config=config)

        # Handle errors - send to monitoring/retry queue
        op.inspect("log_errors", iceberg_output.errs)
        ```
    """

    oks: Stream[IcebergSinkMessage]
    """Successfully processed items."""

    errs: Stream[IcebergError]
    """Errors that occurred during processing."""


# SerDe interfaces for Iceberg record processing


class IcebergSerDe:
    """Base SerDe interface for Iceberg record serialization/deserialization."""
    
    def serialize(self, data: Any) -> Dict[str, Any]:
        """Serialize input to Iceberg record format."""
        raise NotImplementedError
    
    def deserialize(self, record: Dict[str, Any]) -> Any:
        """Deserialize Iceberg record to application format."""
        raise NotImplementedError


class JsonSerDe(IcebergSerDe):
    """
    JSON SerDe for JSON serialization/deserialization in Iceberg records.

    **What**: Converts between Python objects and JSON within Iceberg records
    **Why**: JSON is common for structured data in analytics pipelines
    **When**: Processing structured data, event streams, analytics data

    **Production Use Case**:
    - Event processing pipelines
    - Analytics data transformation
    - Microservices data integration
    - Schema evolution with JSON flexibility

    Example:
        ```python
        # Process JSON events for analytics
        json_serde = JsonSerDe()
        json_stream = iop.serialize_records("serialize", input_stream, json_serde)

        def enrich_event(msg):
            data = msg.data
            data['processed_at'] = datetime.now(tz=timezone.utc).isoformat()
            data['processor_version'] = '2.1.0'
            return IcebergSinkMessage(data=data)

        enriched = op.map("enrich", json_stream.oks, enrich_event)
        ```
    """

    def __init__(self, json_field: str = "json_data"):
        """Initialize JsonSerDe with field name for JSON data."""
        self.json_field = json_field

    def serialize(self, data: Any) -> Dict[str, Any]:
        """Serialize input to Iceberg record with JSON field."""
        if isinstance(data, dict):
            return data
        try:
            return {self.json_field: json.dumps(data) if data is not None else None}
        except (TypeError, ValueError) as e:
            raise ValueError(f"Failed to serialize to JSON: {e}") from e

    def deserialize(self, record: Dict[str, Any]) -> Any:
        """Deserialize JSON field from Iceberg record."""
        json_data = record.get(self.json_field)
        if json_data is None:
            return None
        try:
            return json.loads(json_data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Failed to deserialize JSON: {e}") from e


class ParquetSerDe(IcebergSerDe):
    """
    Direct Parquet-compatible SerDe for high-performance scenarios.

    **What**: Ensures data is compatible with Parquet/Arrow format
    **Why**: Optimal performance for analytical workloads
    **When**: High-throughput analytics, data warehouse scenarios

    **Production Use Case**:
    - High-volume analytics pipelines
    - Data warehouse ETL processes
    - Time-series data processing
    - Petabyte-scale data ingestion

    Example:
        ```python
        # High-performance analytics processing
        parquet_serde = ParquetSerDe()
        analytics_stream = iop.serialize_records("serialize", metrics_stream, parquet_serde)

        def optimize_for_analytics(msg):
            data = msg.data
            # Ensure proper data types for Parquet
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            data['value'] = float(data['value'])
            return IcebergSinkMessage(data=data)

        optimized = op.map("optimize", analytics_stream.oks, optimize_for_analytics)
        ```
    """

    def serialize(self, data: Any) -> Dict[str, Any]:
        """Serialize input ensuring Parquet compatibility."""
        if isinstance(data, dict):
            # Ensure data is Parquet-compatible
            cleaned_data = {}
            for key, value in data.items():
                if value is None:
                    cleaned_data[key] = value
                elif isinstance(value, (int, float, str, bool)):
                    cleaned_data[key] = value
                elif isinstance(value, (list, dict)):
                    # Convert complex types to JSON strings for Parquet
                    cleaned_data[key] = json.dumps(value)
                else:
                    cleaned_data[key] = str(value)
            return cleaned_data
        elif isinstance(data, (int, float, str, bool)):
            return {"value": data}
        else:
            return {"data": json.dumps(data)}

    def deserialize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize Parquet record (typically no transformation needed)."""
        return record


class PickleSerDe(IcebergSerDe):
    """
    Pickle SerDe for complex Python objects in Iceberg.

    **What**: Serializes Python objects using pickle in Iceberg records
    **Why**: Allows complex Python objects in analytics pipelines
    **When**: Internal Python processing, ML pipelines, complex state

    **⚠️ Security Warning**: Only use with trusted data sources.

    Example:
        ```python
        # Process complex ML objects
        pickle_serde = PickleSerDe()
        ml_stream = iop.serialize_records("serialize", model_stream, pickle_serde)
        ```
    """

    def __init__(self, pickle_field: str = "pickle_data"):
        """Initialize PickleSerDe with field name for pickled data."""
        self.pickle_field = pickle_field

    def serialize(self, data: Any) -> Dict[str, Any]:
        """Serialize input using pickle in Iceberg record."""
        import base64
        return {
            self.pickle_field: base64.b64encode(pickle.dumps(data)).decode('utf-8'),
            "type": "pickle"
        }

    def deserialize(self, record: Dict[str, Any]) -> Any:
        """Deserialize pickle field from Iceberg record."""
        import base64
        pickle_data = record.get(self.pickle_field)
        if pickle_data is None:
            return None
        return pickle.loads(base64.b64decode(pickle_data.encode('utf-8')))


# Default SerDe instances for common use cases
DEFAULT_JSON_SERDE = JsonSerDe()
"""Default JSON SerDe - structured data processing."""

DEFAULT_PARQUET_SERDE = ParquetSerDe()
"""Default Parquet SerDe - high-performance analytics."""

DEFAULT_PICKLE_SERDE = PickleSerDe()
"""Default pickle SerDe - complex Python objects (use with caution)."""


@operator
def _iceberg_error_split(
    step_id: str, up: Stream[Union[IcebergSinkMessage, IcebergError]]
) -> IcebergOpOut:
    """Split the stream between successful messages and errors.

    **Internal Operator**: This is used internally by other operators to separate
    successful records from errors, enabling proper error handling.

    Args:
        step_id: Unique identifier for this operator step
        up: Stream containing both successful messages and errors

    Returns:
        IcebergOpOut with separated oks and errs streams
    """
    branch = op.branch(
        step_id.replace(".", "__"),
        up,
        lambda msg: isinstance(msg, IcebergSinkMessage),
    )
    # Cast the streams to the proper expected types.
    oks = cast(Stream[IcebergSinkMessage], branch.trues)
    errs = cast(Stream[IcebergError], branch.falses)
    return IcebergOpOut(oks, errs)


@operator
def serialize_records(
    step_id: str,
    up: Stream[Any],
    serde: Optional[IcebergSerDe] = None,
) -> IcebergOpOut:
    """Serialize data into Iceberg record format using SerDe.

    **What**: Converts application data to IcebergSinkMessage using SerDe
    **Why**: Enables structured data processing with proper error handling
    **When**: Use when you need to process structured data for Iceberg

    **Production Benefits**:
    - Type-safe serialization with proper error handling
    - Error stream separation for monitoring failed serializations
    - Supports various data formats (JSON, Parquet, Pickle)
    - Preserves processing metadata

    Args:
        step_id: Unique identifier for this serialization step
        up: Stream of application data to serialize
        serde: SerDe instance to use (defaults to ParquetSerDe)

    Returns:
        IcebergOpOut with separated success and error streams

    Example:
        ```python
        # Serialize analytics events with error handling
        analytics_stream = iop.serialize_records(
            "serialize_events", 
            raw_events, 
            iop.DEFAULT_JSON_SERDE
        )

        # Process successful serializations
        iop.output("analytics_sink", analytics_stream.oks, config=config)

        # Handle serialization errors
        op.inspect("log_serialize_errors", analytics_stream.errs)
        ```
    """
    if serde is None:
        serde = DEFAULT_PARQUET_SERDE

    def shim_mapper(data: Any) -> Union[IcebergSinkMessage, IcebergError]:
        try:
            serialized_data = serde.serialize(data)
            return IcebergSinkMessage(
                data=serialized_data,
                event_timestamp=int(time.time() * 1000)
            )
        except Exception as e:
            return IcebergError(
                error=f"Serialization failed: {e}",
                record=IcebergSinkMessage(
                    data={"error": "serialization_failed", "original": str(data)[:1000]}
                )
            )

    mapped = op.map(step_id.replace(".", "__"), up, shim_mapper)
    return _iceberg_error_split(f"{step_id}_split", mapped)


@operator
def validate_schema(
    step_id: str,
    up: Stream[Any],
    schema_validator: Optional[callable] = None,
) -> IcebergOpOut:
    """Validate data against a schema before processing.

    **What**: Validates incoming data against a schema
    **Why**: Ensures data quality and prevents downstream errors
    **When**: Use when data quality is critical for analytics

    **Production Benefits**:
    - Early error detection and data quality enforcement
    - Prevents corrupt data from reaching Iceberg tables
    - Enables monitoring of data quality issues
    - Supports schema evolution patterns

    Args:
        step_id: Unique identifier for this validation step
        up: Stream of data to validate
        schema_validator: Function to validate data (returns bool or raises exception)

    Returns:
        IcebergOpOut with separated valid and invalid data

    Example:
        ```python
        def validate_user_event(data):
            required_fields = ['user_id', 'event_type', 'timestamp']
            for field in required_fields:
                if field not in data:
                    raise ValueError(f"Missing required field: {field}")
            return True

        validated_stream = iop.validate_schema(
            "validate_events",
            input_stream, 
            validate_user_event
        )

        # Process valid data
        processed = op.map("process", validated_stream.oks, process_user_event)

        # Handle validation errors
        op.inspect("log_validation_errors", validated_stream.errs)
        ```
    """
    def default_validator(data):
        """Default validator - just checks if data is a dict."""
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary for Iceberg records")
        return True

    validator = schema_validator or default_validator

    def shim_mapper(data: Any) -> Union[IcebergSinkMessage, IcebergError]:
        try:
            validator(data)
            return IcebergSinkMessage(
                data=data if isinstance(data, dict) else {"value": data},
                event_timestamp=int(time.time() * 1000)
            )
        except Exception as e:
            return IcebergError(
                error=f"Schema validation failed: {e}",
                record=IcebergSinkMessage(
                    data={"error": "validation_failed", "original": str(data)[:1000]}
                )
            )

    mapped = op.map(step_id.replace(".", "__"), up, shim_mapper)
    return _iceberg_error_split(f"{step_id}_split", mapped)


@operator
def output(
    step_id: str,
    up: Stream[Union[IcebergSinkMessage, Any]],
    *,
    config: IcebergSinkConfig,
) -> None:
    """Output to Iceberg as a sink with full configuration.

    **What**: Creates an Iceberg sink that writes to specified table
    **Why**: Provides a comprehensive way to output processed records to Iceberg
    **When**: Use for production output scenarios with full configuration control

    **Production Benefits**:
    - Automatic record type conversion
    - Configurable performance parameters
    - Built-in error handling
    - Support for petabyte-scale processing

    Args:
        step_id: Unique identifier for this output step
        up: Stream of records to output
        config: IcebergSinkConfig with full configuration

    Example:
        ```python
        # High-throughput analytics output
        config = IcebergSinkConfig(
            catalog_uri="thrift://hive-metastore:9083",
            table_name="analytics.user_events",
            warehouse_path="s3://data-lake/warehouse",
            catalog_type="hive"
        ).for_high_throughput()

        iop.output("analytics_sink", processed_events, config=config)

        # Petabyte-scale configuration
        petabyte_config = config.for_petabyte_scale()
        iop.output("massive_sink", massive_data_stream, config=petabyte_config)
        ```
    """
    def ensure_iceberg_message(item: Union[IcebergSinkMessage, Any]) -> IcebergSinkMessage:
        """Convert any input to IcebergSinkMessage."""
        if isinstance(item, IcebergSinkMessage):
            return item
        elif isinstance(item, dict):
            return IcebergSinkMessage(data=item)
        else:
            return IcebergSinkMessage(data={"value": item})

    converted = op.map(
        step_id.replace(".", "__"),
        up,
        ensure_iceberg_message,
    )
    
    from .connector import IcebergSink  # Import here to avoid circular imports
    
    return op.output(
        f"{step_id}_iceberg_output",
        converted,
        IcebergSink(config),
    )


@operator
def enrich_with_metadata(
    step_id: str,
    up: Stream[IcebergSinkMessage],
    enricher: callable,
) -> IcebergOpOut:
    """Enrich Iceberg messages with additional metadata.

    **What**: Adds metadata to Iceberg records using a custom enricher function
    **Why**: Enables data enrichment patterns common in analytics pipelines
    **When**: Use when you need to add processing metadata, timestamps, or derived fields

    **Production Benefits**:
    - Standardized metadata enrichment patterns
    - Error handling for enrichment failures
    - Supports lineage and audit trails
    - Enables data governance patterns

    Args:
        step_id: Unique identifier for this enrichment step
        up: Stream of IcebergSinkMessage to enrich
        enricher: Function that takes and returns IcebergSinkMessage

    Returns:
        IcebergOpOut with separated enriched and error records

    Example:
        ```python
        def add_processing_metadata(msg: IcebergSinkMessage) -> IcebergSinkMessage:
            msg.data.update({
                'processed_at': datetime.now(tz=timezone.utc).isoformat(),
                'processor_version': '2.1.0',
                'processing_pipeline': 'analytics-v2'
            })
            return msg

        enriched_stream = iop.enrich_with_metadata(
            "enrich_metadata",
            input_stream,
            add_processing_metadata
        )

        # Output enriched data
        iop.output("enriched_sink", enriched_stream.oks, config=config)

        # Monitor enrichment errors
        op.inspect("log_enrichment_errors", enriched_stream.errs)
        ```
    """
    def shim_mapper(msg: IcebergSinkMessage) -> Union[IcebergSinkMessage, IcebergError]:
        try:
            return enricher(msg)
        except Exception as e:
            return IcebergError(
                error=f"Enrichment failed: {e}",
                record=msg
            )

    mapped = op.map(step_id.replace(".", "__"), up, shim_mapper)
    return _iceberg_error_split(f"{step_id}_split", mapped) 