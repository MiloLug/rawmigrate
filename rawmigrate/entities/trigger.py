from typing import TYPE_CHECKING, override

from rawmigrate.core import SqlText, SqlTextLike
from rawmigrate.entity import DBEntity
from rawmigrate.core import SqlIdentifier

if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


class Trigger(SqlIdentifier, DBEntity):
    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
        name: str,
        before: str | None,
        after: str | None,
        instead_of: str | None,
        on: SqlText,
        function: SqlText | None,
        procedure: SqlText | None,
    ):
        if not function and not procedure:
            raise ValueError("Either function or procedure must be provided")

        if not any([before, after, instead_of]):
            raise ValueError(
                "At least one of before, after, or instead_of must be provided"
            )

        self.name = name
        self.before = before
        self.after = after
        self.instead_of = instead_of
        self.on = on
        self.function = function
        self.procedure = procedure
        DBEntity.__init__(self, manager, entity_ref)
        SqlIdentifier.__init__(self, manager.db.syntax, [name], [entity_ref])

    @classmethod
    def create(
        cls,
        _manager: "EntityManager",
        _name: str,
        _entity_ref: str = "",
        *,
        before: str | None = None,
        after: str | None = None,
        instead_of: str | None = None,
        on: SqlTextLike,
        function: SqlTextLike | None = None,
        procedure: SqlTextLike | None = None,
    ):
        entity_ref = _entity_ref or cls.create_ref(_manager, _name)
        return cls(
            manager=_manager,
            entity_ref=entity_ref,
            name=_name,
            before=before,
            after=after,
            instead_of=instead_of,
            on=SqlText(_manager.db.syntax, on),
            function=SqlText(_manager.db.syntax, function) if function else None,
            procedure=SqlText(_manager.db.syntax, procedure) if procedure else None,
        )

    def _infer_dependency_refs(self) -> set[str]:
        return set.union(
            self.on.references,
            self.function.references if self.function else set(),
            self.procedure.references if self.procedure else set(),
        )

    @override
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "before": self.before,
            "after": self.after,
            "instead_of": self.instead_of,
            "on": self.on.sql,
            "function": self.function.sql if self.function else "",
            "procedure": self.procedure.sql if self.procedure else "",
        }

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", entity_ref: str, data: dict):
        return cls(
            manager=manager,
            entity_ref=entity_ref,
            name=data["name"],
            before=data["before"],
            after=data["after"],
            instead_of=data["instead_of"],
            on=SqlText(manager.db.syntax, data["on"]),
            function=(
                SqlText(manager.db.syntax, data["function"])
                if data["function"]
                else None
            ),
            procedure=(
                SqlText(manager.db.syntax, data["procedure"])
                if data["procedure"]
                else None
            ),
        )
