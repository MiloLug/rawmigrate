from abc import ABC, abstractmethod

from typing import TYPE_CHECKING, override

from rawmigrate.utils import hash_str


if TYPE_CHECKING:
    from rawmigrate.entities.schema import Schema
    from rawmigrate.entity_manager import EntityManager


class DBEntity(ABC):
    def __init__(
        self, manager: "EntityManager", entity_ref: str, dependencies: set[str] | None
    ):
        self._manager = manager
        self._entity_ref = entity_ref
        self._explicit_dependencies = dependencies or set()

    @classmethod
    @abstractmethod
    def create[R: DBEntity](cls: type[R], *args, **kwargs) -> R: ...

    @classmethod
    def create_ref(cls: type["DBEntity"], name: str, *args, **kwargs) -> str:
        return f"{cls.__name__}:{name}"

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
        return self._infer_dependency_refs() | self._explicit_dependencies

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
        cls: type[R], manager: "EntityManager", data: dict
    ) -> R: ...

    def __hash__(self) -> int:
        return hash_str(self.ref)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DBEntity):
            return False
        return self.ref == other.ref


class SchemaDependantEntity(DBEntity):
    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
        schema: "Schema | None",
        dependencies: set[str] | None,
    ):
        super().__init__(manager, entity_ref, dependencies)
        self._schema = schema

    @classmethod
    def create_ref(
        cls: type["SchemaDependantEntity"],
        name: str,
        schema: "Schema | None",
        *args,
        **kwargs,
    ) -> str:
        schema_prefix = f"{schema.ref}|" if schema else ""
        return f"{schema_prefix}{super().create_ref(name, *args, **kwargs)}"

    @override
    @property
    def dependency_refs(self) -> set[str]:
        return super().dependency_refs or (
            {self._schema.ref} if self._schema else set()
        )
