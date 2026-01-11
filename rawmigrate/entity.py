from abc import ABC, abstractmethod

from typing import TYPE_CHECKING

from rawmigrate.utils import hash_str


if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


class DBEntity(ABC):
    def __init__(self, manager: "EntityManager", entity_ref: str):
        self._manager = manager
        self._entity_ref = entity_ref

    @classmethod
    @abstractmethod
    def create[R: DBEntity](cls: type[R], *args, **kwargs) -> R: ...

    @classmethod
    def create_ref[T: DBEntity](
        cls: type[T], manager: "EntityManager", name: str, use_schema: bool = True
    ) -> str:
        schema = f"{manager.schema.ref}." if use_schema and manager.schema else ""
        return f"{schema}{cls.__name__}:{name}"

    @property
    def ref(self) -> str:
        """
        Returns the entity ID.
        This should be a deterministic value that won't ever change,
            unless the entity is about to be dropped or recreated.
        """
        return self._entity_ref

    @property
    def dependency_refs(self) -> set[str]:
        return self._infer_dependency_refs() | self._manager.dependency_refs

    @property
    def manager(self) -> "EntityManager":
        return self._manager

    @abstractmethod
    def _infer_dependency_refs(self) -> set[str]: ...

    @property
    def then(self) -> "EntityManager":
        return self._manager.after(self)

    @abstractmethod
    def to_dict(self) -> dict: ...

    @classmethod
    @abstractmethod
    def from_dict[R: DBEntity](
        cls: type[R], manager: "EntityManager", entity_ref: str, data: dict
    ) -> R: ...

    def __hash__(self) -> int:
        return hash_str(self.ref)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DBEntity):
            return False
        return self.ref == other.ref
