from typing import Iterator, Self, override, cast

from typing import TYPE_CHECKING

from rawmigrate.core import SqlText, SqlTextLike
from rawmigrate.entity import EntityBundle, SchemaDependantEntity
from rawmigrate.entity import DBEntity
from rawmigrate.core import SqlIdentifier

if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


# TODO: IMPORTANT: This is a temporary implementation of column, IMPLEMENT THE FULL ONE


class Column(SqlIdentifier, DBEntity):
    manage_export = False

    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
        dependencies: set[str] | None,
        table_ref: str,
        name: str,
        definition: SqlText,
    ):
        self.table_ref = table_ref
        self.name = name
        self.definition = definition
        DBEntity.__init__(self, manager, entity_ref, dependencies)
        SqlIdentifier.__init__(self, manager.db.syntax, [name], [entity_ref])

    @classmethod
    def create(
        cls,
        _manager: "EntityManager",
        table_ref: str,
        name: str,
        definition: SqlTextLike,
    ):
        cleaned_definition = SqlText(_manager.db.syntax, definition)
        return cls(
            manager=_manager,
            entity_ref=f"{table_ref}|{cls.create_ref(name)}",
            dependencies={table_ref} | cleaned_definition.references,
            table_ref=table_ref,
            name=name,
            definition=cleaned_definition,
        )

    @override
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "ref": self.ref,
            "definition": self.definition.sql,
            "dependencies": list(self.dependency_refs - {self.table_ref}),
        }

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", data: dict):
        return cls(
            manager=manager,
            entity_ref=data["ref"],
            table_ref=data["table_ref"],
            dependencies=set(data["dependencies"]),
            name=data["name"],
            definition=SqlText(manager.db.syntax, data["definition"]),
        )


class TableColumnsAccessor:
    def __init__(self, table: "Table"):
        self.table = table

    def __getitem__(self, name: str) -> Column:
        return cast(
            Column, self.table.manager.registry.get_entity(self.table._columns[name])
        )

    def __getattr__(self, name: str) -> Column:
        return cast(
            Column, self.table.manager.registry.get_entity(self.table._columns[name])
        )

    def __iter__(self) -> Iterator[tuple[str, Column]]:
        return cast(
            Iterator[tuple[str, Column]],
            (
                (name, self.table.manager.registry.get_entity(ref))
                for name, ref in self.table._columns.items()
            ),
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
        columns: dict[str, str],
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
            name: Column.create(_manager, entity_ref, name, definition)
            for name, definition in columns.items()
        }

        table = cls(
            manager=_manager,
            entity_ref=entity_ref,
            schema=_manager.schema,
            dependencies=(
                _manager.dependency_refs.union(
                    *(column.dependency_refs for column in column_entities.values())
                )
                - {entity_ref}
            ),
            name=_name,
            columns={name: column.ref for name, column in column_entities.items()},
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

    @override
    def _infer_dependency_refs(self) -> set[str]:
        deps = set[str]().union(
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
                column_name: self.manager.registry.get_entity(ref).to_dict()
                for column_name, ref in self._columns.items()
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
                    col_name: col_data["ref"]
                    for col_name, col_data in data["columns"].items()
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
