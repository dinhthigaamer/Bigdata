"""Spark StructType schemas derived directly from common/ Pydantic models."""
from datetime import datetime
from typing import Any, Literal, Union, get_args, get_origin

try:
    from types import UnionType
    _UNION_TYPES = (Union, UnionType)
except ImportError:  # Python < 3.10
    _UNION_TYPES = (Union,)

from pydantic import BaseModel
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from common.envelope import (
    SCHEMA_VERSION,
    StationInfoEnvelope,
    StationStatusEnvelope,
    WeatherEnvelope,
)
from common.schemas.station_info import StationInfo
from common.schemas.station_status import StationStatus, VehicleCount
from common.schemas.weather import WeatherMessage

ENVELOPE_MAJOR_VERSION = SCHEMA_VERSION.split(".")[0]

_PRIMITIVE_TYPES: dict[type, DataType] = {
    str: StringType(),
    int: IntegerType(),
    float: DoubleType(),
    bool: BooleanType(),
    # Parse ISO 8601 timestamps as strings first so mixed precision (.ffffffZ vs Z) never fails
    datetime: StringType(),
}


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    """Return (inner_annotation, is_nullable) for Optional/Union[..., None]."""
    origin = get_origin(annotation)
    if origin in _UNION_TYPES:
        args = get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0], True
    return annotation, False


def _annotation_to_spark_type(annotation: Any) -> DataType:
    """Map a Python/Pydantic type annotation to a PySpark DataType."""
    inner, _ = _unwrap_optional(annotation)
    origin = get_origin(inner)

    if inner in _PRIMITIVE_TYPES:
        return _PRIMITIVE_TYPES[inner]

    if isinstance(inner, type) and issubclass(inner, BaseModel):
        return pydantic_to_spark_struct(inner)

    if origin is Literal:
        first_val = get_args(inner)[0]
        return _PRIMITIVE_TYPES.get(type(first_val), StringType())

    if origin in (list, tuple):
        item_type = get_args(inner)[0] if get_args(inner) else str
        return ArrayType(_annotation_to_spark_type(item_type))

    if origin is dict or inner is dict:
        return MapType(StringType(), StringType())

    return StringType()


def pydantic_to_spark_struct(model_cls: type[BaseModel]) -> StructType:
    """Convert a Pydantic v2 BaseModel class into a PySpark StructType."""
    fields: list[StructField] = []
    for name, field_info in model_cls.model_fields.items():
        spark_type = _annotation_to_spark_type(field_info.annotation)
        # Spark JSON parsing handles missing/null fields gracefully when nullable=True
        fields.append(StructField(name, spark_type, nullable=True))
    return StructType(fields)


VEHICLE_COUNT_SCHEMA = pydantic_to_spark_struct(VehicleCount)
STATION_STATUS_PAYLOAD_SCHEMA = pydantic_to_spark_struct(StationStatus)
STATION_STATUS_ENVELOPE_SCHEMA = pydantic_to_spark_struct(StationStatusEnvelope)

STATION_INFO_PAYLOAD_SCHEMA = pydantic_to_spark_struct(StationInfo)
STATION_INFO_ENVELOPE_SCHEMA = pydantic_to_spark_struct(StationInfoEnvelope)

WEATHER_PAYLOAD_SCHEMA = pydantic_to_spark_struct(WeatherMessage)
WEATHER_ENVELOPE_SCHEMA = pydantic_to_spark_struct(WeatherEnvelope)
