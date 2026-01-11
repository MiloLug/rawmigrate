from typing import TYPE_CHECKING, override

from rawmigrate.core import SqlText, SqlTextLike
from rawmigrate.entity import DBEntity
from rawmigrate.core import SqlIdentifier

if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


class Index(SqlIdentifier, DBEntity):
    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
        name: str,
        on: SqlText,
        using: SqlText,
        expressions: list[SqlText],
    ):
        self.name = name
        self.on = on
        self.using = using
        self.expressions = expressions
        DBEntity.__init__(self, manager, entity_ref)
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
        return cls(
            manager=_manager,
            entity_ref=_entity_ref or cls.create_ref(_manager, _name),
            name=_name,
            on=SqlText(_manager.db.syntax, on),
            using=SqlText(_manager.db.syntax, using),
            expressions=[
                SqlText(_manager.db.syntax, expression) for expression in expressions
            ],
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
            "on": self.on.sql,
            "using": self.using.sql,
            "expressions": [expression.sql for expression in self.expressions],
        }

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", entity_ref: str, data: dict):
        return cls(
            manager=manager,
            entity_ref=entity_ref,
            name=data["name"],
            on=SqlText(manager.db.syntax, data["on"]),
            using=SqlText(manager.db.syntax, data["using"]),
            expressions=[
                SqlText(manager.db.syntax, expression)
                for expression in data["expressions"]
            ],
        )
