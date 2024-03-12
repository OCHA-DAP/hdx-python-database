"""Utilities for database datetime columns without timezone"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, TypeDecorator
from sqlalchemy.orm import DeclarativeBase, declared_attr

from .utils import camel_to_snake_case


class ConversionNoTZ(TypeDecorator):
    """Convert from/to datetime with timezone from database columns that don't
    use timezone"""

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: Optional[datetime], _):
        if value is None:
            return value
        if value.tzinfo:
            value = value.astimezone(timezone.utc)

        return value.replace(tzinfo=None)

    def process_result_value(self, value, _):
        if value is None:
            return value
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)


class Base(DeclarativeBase):
    type_annotation_map = {
        datetime: ConversionNoTZ,
    }

    @declared_attr.directive
    def __tablename__(cls):
        return camel_to_snake_case(cls.__name__)
