from typing import TYPE_CHECKING, override

from rawmigrate.core import SqlText, SqlTextLike
from rawmigrate.entity import DBEntity, EntityBundle
from rawmigrate.core import SqlIdentifier

if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


class Index(SqlIdentifier, DBEntity):
    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
        dependencies: set[str] | None,
        name: str,
        on: SqlText,
        using: SqlText,
        expressions: list[SqlText],
    ):
        self.name = name
        self.on = on
        self.using = using
        self.expressions = expressions
        DBEntity.__init__(self, manager, entity_ref, dependencies)
        SqlIdentifier.__init__(self, manager.db.syntax, [name], [entity_ref])

    @classmethod
    def create(
        cls,
        _manager: "EntityManager",
        _name: str,
        _entity_ref: str = "",
        *,
        on: SqlTextLike,
        using: SqlTextLike,
        expressions: list[SqlTextLike],
    ):
        return EntityBundle(
            cls(
                manager=_manager,
                entity_ref=_entity_ref or cls.create_ref(_name),
                dependencies=_manager.dependency_refs,
                name=_name,
                on=SqlText(_manager.db.syntax, on),
                using=SqlText(_manager.db.syntax, using),
                expressions=[
                    SqlText(_manager.db.syntax, expression)
                    for expression in expressions
                ],
            )
        )

    def _infer_dependency_refs(self) -> set[str]:
        return set.union(
            self.on.references,
            self.using.references,
            *(expression.references for expression in self.expressions),
        )

    @override
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "ref": self.ref,
            "on": self.on.sql,
            "using": self.using.sql,
            "expressions": [expression.sql for expression in self.expressions],
            "dependencies": list(self.dependency_refs),
        }

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", data: dict):
        return EntityBundle(
            cls(
                manager=manager,
                entity_ref=data["ref"],
                dependencies=set(data["dependencies"]),
                name=data["name"],
                on=SqlText(manager.db.syntax, data["on"]),
                using=SqlText(manager.db.syntax, data["using"]),
                expressions=[
                    SqlText(manager.db.syntax, expression)
                    for expression in data["expressions"]
                ],
            )
        )
