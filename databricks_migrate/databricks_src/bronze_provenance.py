"""Shared Spark expressions for the versioned Bronze provenance contract."""

from __future__ import annotations

import re
from typing import Iterable


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

EXTENDED_PROVENANCE_COLUMNS: tuple[str, ...] = (
    "_source_record_id",
    "_source_published_at",
    "_payload_hash",
)

EXTENDED_PROVENANCE_TYPES: dict[str, str] = {
    "_source_record_id": "STRING",
    "_source_published_at": "TIMESTAMP",
    "_payload_hash": "STRING",
}


def validate_column_names(columns: Iterable[str]) -> tuple[str, ...]:
    result = tuple(columns)
    if not result:
        raise ValueError("columns cannot be empty")
    if len(result) != len(set(result)):
        raise ValueError("columns cannot contain duplicates")
    invalid = [column for column in result if not _IDENTIFIER.fullmatch(column)]
    if invalid:
        raise ValueError(f"Unsafe column identifier(s): {invalid}")
    return result


def source_record_id_expression(dataset: str, key_columns: Iterable[str]):
    """Stable SHA-256 identity for the source row represented by a Bronze key."""
    from pyspark.sql import functions as F

    keys = validate_column_names(key_columns)
    parts = [F.lit(dataset)] + [
        F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
        for column in keys
    ]
    return F.sha2(F.concat_ws("\u001f", *parts), 256)


def payload_hash_expression(columns: Iterable[str]):
    """SHA-256 of canonical JSON over non-metadata business columns."""
    from pyspark.sql import functions as F

    all_columns = tuple(columns)
    if not all_columns:
        raise ValueError("columns cannot be empty")
    if len(all_columns) != len(set(all_columns)):
        raise ValueError("columns cannot contain duplicates")
    if any(not isinstance(column, str) or not column for column in all_columns):
        raise ValueError("column names must be non-empty strings")

    # FinLab source fields may use Chinese names, spaces, or punctuation.  Spark's
    # Column API can address them safely when the names are quoted; they do not
    # need to satisfy the SQL-identifier rule used for primary keys.
    business_columns = tuple(
        sorted(column for column in all_columns if not column.startswith("_"))
    )
    if not business_columns:
        raise ValueError("payload hash requires at least one business column")
    payload = F.to_json(
        F.struct(
            *[
                F.col(f"`{column.replace('`', '``')}`").alias(column)
                for column in business_columns
            ]
        ),
        options={"ignoreNullFields": "false"},
    )
    return F.sha2(payload, 256)

