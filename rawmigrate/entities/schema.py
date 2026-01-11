from typing import TYPE_CHECKING, override
from rawmigrate.core import SqlIdentifier
from rawmigrate.entity import DBEntity

if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


class Schema(SqlIdentifier, DBEntity):
    def __init__(self, manager: "EntityManager", entity_ref: str, name: str):
        self.name = name
        DBEntity.__init__(self, manager, entity_ref)
        SqlIdentifier.__init__(self, manager.db.syntax, [name], [entity_ref])

    @classmethod
    def create(
        cls,
        _manager: "EntityManager",
        _name: str,
        _entity_ref: str = "",
    ):
        return cls(
            manager=_manager,
            entity_ref=_entity_ref or cls.create_ref(_manager, _name, use_schema=False),
            name=_name,
        )

    @property
    def schema(self) -> "Schema | None":
        return self._manager.schema

    @override
    def _infer_dependency_refs(self) -> set[str]:
        return set()

    @override
    def to_dict(self) -> dict:
        return {"name": self.name}

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", entity_ref: str, data: dict):
        return cls(
            manager=manager,
            entity_ref=entity_ref,
            name=data["name"],
        )
