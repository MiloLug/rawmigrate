from typing import Iterator, Self, override

from typing import TYPE_CHECKING

from rawmigrate.core import SqlText, SqlTextLike
from rawmigrate.entity import DBEntity
from rawmigrate.core import SqlIdentifier

if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


class TableColumnsAccessor:
    def __init__(self, table: "Table"):
        self.table = table

    def __getitem__(self, name: str) -> SqlIdentifier:
        return self.table._columns[name][0]

    def __getattr__(self, name: str) -> SqlIdentifier:
        return self.table._columns[name][0]

    def __iter__(self) -> Iterator[tuple[str, SqlIdentifier, SqlText]]:
        return (
            (col_name, identifier, text)
            for col_name, (identifier, text) in self.table._columns.items()
        )

    def __len__(self) -> int:
        return len(self.table._columns)


class Table(SqlIdentifier, DBEntity):
    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
        name: str,
        columns: dict[str, tuple[SqlIdentifier, SqlText]],
        additional_expressions: list[SqlText],
    ):
        self._name = name
        self._columns = columns
        self._additional_expressions = additional_expressions
        self.c = TableColumnsAccessor(self)
        DBEntity.__init__(self, manager, entity_ref)
        SqlIdentifier.__init__(self, manager.db.syntax, [name], [entity_ref])

    @classmethod
    def create(
        cls,
        _manager: "EntityManager",
        _name: str,
        _entity_ref: str = "",
        _table_expressions: list[SqlTextLike] = [],
        **columns: SqlTextLike,
    ):
        entity_ref = _entity_ref or cls.create_ref(_manager, _name)
        return cls(
            manager=_manager,
            entity_ref=entity_ref,
            name=_name,
            columns={
                column_name: (
                    SqlIdentifier(_manager.db.syntax, [column_name], [entity_ref]),
                    SqlText(_manager.db.syntax, column_definition),
                )
                for column_name, column_definition in columns.items()
            },
            additional_expressions=[
                SqlText(_manager.db.syntax, expression)
                for expression in _table_expressions
            ],
        )

    def additional(self, *expressions: SqlTextLike) -> Self:
        self._additional_expressions.extend(
            SqlText(self._manager.db.syntax, expression) for expression in expressions
        )
        self._manager.update_refs(self)
        return self

    def _infer_dependency_refs(self) -> set[str]:
        deps = set[str]().union(
            *(value.references for _, value in self._columns.values()),
            *(expression.references for expression in self._additional_expressions),
        )
        if not deps and self._manager.schema:
            deps.add(self._manager.schema.ref)

        return deps

    @override
    def to_dict(self) -> dict:
        return {
            "name": self._name,
            "columns": {
                column_name: text.sql
                for column_name, (_, text) in self._columns.items()
            },
            "additional_expressions": [
                expression.sql for expression in self._additional_expressions
            ],
        }

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", entity_ref: str, data: dict):
        return cls(
            manager=manager,
            entity_ref=entity_ref,
            name=data["name"],
            columns={
                column_name: (
                    SqlIdentifier(manager.db.syntax, [column_name], [entity_ref]),
                    SqlText(manager.db.syntax, text),
                )
                for column_name, text in data["columns"].items()
            },
            additional_expressions=[
                SqlText(manager.db.syntax, expression)
                for expression in data["additional_expressions"]
            ],
        )
