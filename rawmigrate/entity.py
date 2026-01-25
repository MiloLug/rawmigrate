from abc import ABC, abstractmethod

from typing import TYPE_CHECKING, ClassVar, Iterable, override

from rawmigrate.utils import hash_str


if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


class EntityBundle[T: DBEntity]:
    def __init__(self, main: T, children: Iterable["DBEntity"] = ()):
        self.main = main
        self.children = list(children)

    @property
    def all(self) -> list["DBEntity"]:
        return [self.main, *self.children]


class DBEntity(ABC):
    manage_export: ClassVar[bool] = True

    def __init__(
        self, manager: "EntityManager", entity_ref: str, dependencies: set[str] | None
    ):
        self._manager = manager
        self._entity_ref = entity_ref
        self._explicit_dependencies = dependencies or set()

    @classmethod
    @abstractmethod
    def create[R: DBEntity](cls: type[R], *args, **kwargs) -> EntityBundle[R]: ...

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
        dynamic = self._infer_dependency_refs()
        return (
            (dynamic | self._explicit_dependencies)
            if dynamic
            else self._explicit_dependencies
        )

    @property
    def manager(self) -> "EntityManager":
        return self._manager

    def _infer_dependency_refs(self) -> set[str] | None:
        return None

    @property
    def then(self) -> "EntityManager":
        return self._manager.after(self)

    @abstractmethod
    def to_dict(self) -> dict: ...

    @classmethod
    @abstractmethod
    def from_dict[R: DBEntity](
        cls: type[R], manager: "EntityManager", data: dict
    ) -> EntityBundle[R]: ...

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
        schema: "DBEntity | None",
        dependencies: set[str] | None,
    ):
        super().__init__(manager, entity_ref, dependencies)
        self._schema = schema

    @classmethod
    def create_ref(
        cls: type["SchemaDependantEntity"],
        name: str,
        schema: "DBEntity | None",
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
