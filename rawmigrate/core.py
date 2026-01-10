from abc import ABC
from enum import StrEnum
from typing import Iterable, Sequence

from rawmigrate.utils import hash_str


class Syntax:
    def __init__(self, meta_open: str = "\ue000", meta_close: str = "\ue001"):
        self.meta_open = meta_open
        self.meta_close = meta_close

    def format_sql_identifier(self, parts: Sequence[str]) -> str:
        return f'"{'"."'.join(parts)}"'

    def format_meta_value(self, value: str) -> str:
        return f"{self.meta_open}{value}{self.meta_close}"

    def format_meta_values(self, values: Iterable[str]) -> str:
        return "".join(self.format_meta_value(value) for value in values)

    def extract_meta_tags(self, text: str) -> tuple[str, set[str]]:
        """
        Extracts meta values from the text.

        Returns:
            The text without the meta values, and the list of meta values.
        """

        # example 1: "{meta}sql{meta}" -> ["", "meta}sql", "meta}"]
        # example 2: "sql{meta}" -> ["sql", "meta}"]
        start_parts = text.split(self.meta_open)

        # example 1: ["", "meta}sql", "meta}"] -> [["meta", "sql"], ["meta", ""]]
        # example 2: ["sql", "meta}"] -> [["sql"], ["meta", ""]]
        all_parts = [part.split(self.meta_close) for part in start_parts if part]

        # from examples, follows that the last part is always sql/text, and the first is the value (if 2 parts)
        result_text = "".join(part[-1] for part in all_parts)
        result_tags = {part[0] for part in all_parts if len(part) > 1}

        return result_text, result_tags


class DB:
    def __init__(
        self,
        syntax: Syntax | None = None,
    ):
        self.syntax = syntax or Syntax()
        # just example for now
        self.entity_storage = {}  # type: ignore


class SqlFormatOption(StrEnum):
    """
    Specifies the format of the entity.
    """

    SQL_TEXT = "s"  # suitable for raw SQL text, without any tags
    SQL_META = (
        "m"  # default, for SQL text, but with meta tags for dependency recognition
    )


class BaseSqlText(ABC):
    def __init__(self, syntax: Syntax):
        self._syntax: Syntax = syntax
        self._references: set[str] = set()
        self._sql: str = ""

    @property
    def sql(self) -> str:
        return self._sql

    @property
    def references(self) -> set[str]:
        return self._references

    def __format__(self, format_spec: SqlFormatOption | str) -> str:
        match format_spec:
            case SqlFormatOption.SQL_TEXT:
                return self.sql
            case SqlFormatOption.SQL_META | "":
                if self.references:
                    return (
                        f"{self.sql}{self._syntax.format_meta_values(self.references)}"
                    )
                else:
                    return self.sql
            case _:
                raise ValueError(f"Invalid format spec: {format_spec}")

    def __hash__(self) -> int:
        return hash_str(self.sql)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BaseSqlText):
            return False
        return self.sql == other.sql


type SqlTextLike = str | BaseSqlText


class SqlText(BaseSqlText):
    def __init__(self, syntax: Syntax, text: SqlTextLike):
        super().__init__(syntax)
        if isinstance(text, BaseSqlText):
            self._sql = text.sql
            self._references = text.references
        else:
            self._sql, self._references = self._syntax.extract_meta_tags(text)


class SqlIdentifier(BaseSqlText):
    def __init__(
        self, syntax: Syntax, parts: Sequence[str], references: Iterable[str] = ()
    ):
        super().__init__(syntax)
        self._sql = self._syntax.format_sql_identifier(parts)
        self._references = set(references)
