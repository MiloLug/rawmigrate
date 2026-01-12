from typing import TYPE_CHECKING, override
from rawmigrate.core import SqlIdentifier
from rawmigrate.entity import DBEntity

if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


class Schema(SqlIdentifier, DBEntity):
    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
        dependencies: set[str] | None,
        name: str,
    ):
        self.name = name
        DBEntity.__init__(self, manager, entity_ref, dependencies)
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
            entity_ref=_entity_ref or cls.create_ref(_name),
            dependencies=_manager.dependency_refs,
            name=_name,
        )

    @override
    def _infer_dependency_refs(self) -> set[str]:
        return set()

    @override
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "ref": self.ref,
            "dependencies": list(self.dependency_refs),
        }

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", data: dict):
        return cls(
            manager=manager,
            entity_ref=data["ref"],
            dependencies=set(data["dependencies"]),
            name=data["name"],
        )
