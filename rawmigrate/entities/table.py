from typing import Iterator, Self, override, cast

from typing import TYPE_CHECKING

from rawmigrate.core import SqlText, SqlTextLike
from rawmigrate.entity import EntityBundle, SchemaDependantEntity
from rawmigrate.entity import DBEntity
from rawmigrate.core import SqlIdentifier

if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


class Column(SqlIdentifier, DBEntity):
    manage_export = False

    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
        table_ref: str,
        name: str,
    ):
        self.name = name
        self.table_ref = table_ref
        DBEntity.__init__(self, manager, entity_ref, {table_ref})
        SqlIdentifier.__init__(self, manager.db.syntax, [name], [entity_ref])

    @classmethod
    def create(
        cls,
        _manager: "EntityManager",
        table_ref: str,
        name: str,
    ):
        return cls(_manager, f"{table_ref}|{cls.create_ref(name)}", table_ref, name)

    @override
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "ref": self.ref,
        }

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", data: dict):
        return cls(
            manager=manager,
            entity_ref=data["ref"],
            table_ref=data["table_ref"],
            name=data["name"],
        )


class TableColumnsAccessor:
    def __init__(self, table: "Table"):
        self.table = table

    def __getitem__(self, name: str) -> Column:
        return cast(
            Column, self.table.manager.registry.get_entity(self.table._columns[name][0])
        )

    def __getattr__(self, name: str) -> Column:
        return cast(
            Column, self.table.manager.registry.get_entity(self.table._columns[name][0])
        )

    def __iter__(self) -> Iterator[tuple[str, str, SqlText]]:
        return (
            (col_name, column_ref, text)
            for col_name, (column_ref, text) in self.table._columns.items()
        )

    def __len__(self) -> int:
        return len(self.table._columns)


class Table(SqlIdentifier, SchemaDependantEntity):
    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
        schema: "DBEntity | None",
        dependencies: set[str] | None,
        name: str,
        columns: dict[str, tuple[str, SqlText]],
        additional_expressions: list[SqlText],
    ):
        self._name = name
        self._columns = columns
        self._additional_expressions = additional_expressions
        self.c = TableColumnsAccessor(self)
        SchemaDependantEntity.__init__(self, manager, entity_ref, schema, dependencies)
        SqlIdentifier.__init__(self, manager.db.syntax, [name], [entity_ref])

    @classmethod
    def create(
        cls,
        _manager: "EntityManager",
        _name: str,
        _entity_ref: str = "",
        _table_expressions: list[SqlTextLike] | None = None,
        **columns: SqlTextLike,
    ):
        entity_ref = _entity_ref or cls.create_ref(_name, schema=_manager.schema)
        column_entities = {
            name: Column.create(_manager, entity_ref, name) for name in columns.keys()
        }

        table = cls(
            manager=_manager,
            entity_ref=entity_ref,
            schema=_manager.schema,
            dependencies=_manager.dependency_refs,
            name=_name,
            columns={
                column_name: (
                    column_entities[column_name].ref,
                    SqlText(_manager.db.syntax, column_definition),
                )
                for column_name, column_definition in columns.items()
            },
            additional_expressions=[
                SqlText(_manager.db.syntax, expression)
                for expression in _table_expressions or []
            ],
        )

        return EntityBundle(table, column_entities.values())

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
        return deps

    @override
    def to_dict(self) -> dict:
        return {
            "name": self._name,
            "schema": self._schema.ref if self._schema else None,
            "ref": self.ref,
            "columns": {
                column_name: self.c[column_name].to_dict()
                | {
                    "text": text.sql,
                }
                for column_name, (_, text) in self._columns.items()
            },
            "additional_expressions": [
                expression.sql for expression in self._additional_expressions
            ],
            "dependencies": list(self.dependency_refs),
        }

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", data: dict):
        return EntityBundle(
            cls(
                manager=manager,
                entity_ref=data["ref"],
                schema=manager.registry.get_entity(data["schema"], allow_none=True),
                dependencies=set(data["dependencies"]),
                name=data["name"],
                columns={
                    column_name: (
                        data["ref"],
                        SqlText(manager.db.syntax, data["text"]),
                    )
                    for column_name, data in data["columns"].items()
                },
                additional_expressions=[
                    SqlText(manager.db.syntax, expression)
                    for expression in data["additional_expressions"]
                ],
            ),
            [
                Column.from_dict(manager, column_data | {"table_ref": data["ref"]})
                for column_data in data["columns"].values()
            ],
        )
