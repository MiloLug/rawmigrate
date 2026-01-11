from dataclasses import dataclass
import functools
from typing import TYPE_CHECKING, Callable, Concatenate, Generator, Self
from rawmigrate.core import DB
import graphlib

from rawmigrate.entities.table import Table
from rawmigrate.entities.index import Index
from rawmigrate.entities.function import Function
from rawmigrate.entities.trigger import Trigger
from rawmigrate.entities.schema import Schema

if TYPE_CHECKING:
    from rawmigrate.entity import DBEntity


@dataclass(slots=True, kw_only=True)
class EntityNode:
    entity: DBEntity
    dependencies: set["EntityNode"]
    dependants: set["EntityNode"]

    def __hash__(self) -> int:
        return self.entity.__hash__()

    def __eq__(self, other: object) -> bool:
        return self.entity.__eq__(other)


class EntityRegistry:
    def __init__(self):
        self._registry: dict[str, EntityNode] = dict()
        self._ref_adjacency: dict[str, set[str]] = dict()
        self._roots: set[EntityNode] = set()

    def register(self, entity: DBEntity):
        """
        Register an entity in the registry and update the tree of dependencies.
        """
        dependencies = {
            self._registry[dependency_ref] for dependency_ref in entity.dependency_refs
        }
        node = EntityNode(entity=entity, dependencies=dependencies, dependants=set())
        for dependency in dependencies:
            dependency.dependants.add(node)

        self._registry[entity.ref] = node
        self._ref_adjacency[entity.ref] = entity.dependency_refs.copy()
        if not dependencies:
            self._roots.add(node)

    def update_node(self, entity: DBEntity):
        """
        Update the node of this entity, recomputing dependencies and dependants.
        """
        node = self._registry[entity.ref]
        for dependency in node.dependencies:
            dependency.dependants.discard(node)
        node.dependencies = {
            self._registry[dependency_ref] for dependency_ref in entity.dependency_refs
        }
        for dependency in node.dependencies:
            dependency.dependants.add(node)
        # No need to recompute dependants,
        # since changing a node can't make its dependants not-depend on it

        self._ref_adjacency[entity.ref] = entity.dependency_refs.copy()
        self._roots.discard(node)
        if not node.dependencies:
            self._roots.add(node)

    def topological_order(self) -> Generator[EntityNode]:
        """
        Return topological order of the entities in the registry.
        """
        return (
            self._registry[ref]
            for ref in graphlib.TopologicalSorter(self._ref_adjacency).static_order()
        )

    def get_node(self, ref: str) -> EntityNode:
        return self._registry[ref]


class EntityManager:
    def __init__(
        self,
        parent: "EntityManager | None" = None,
        db: DB | None = None,
        schema: Schema | None = None,
        registry: EntityRegistry | None = None,
        dependencies: set[str] | None = None,
    ):
        """
        Args:
            parent: The parent entity manager
            db: The database to use
                default = parent db, if no parent provided - error
            schema: The schema to use
                default = parent schema or None
            registry: The registry to use
                default = parent registry, if no parent provided - error
            dependencies: Default dependencies to use in entities created by this manager
                default = parent dependencies or empty set
        """
        self._parent = parent
        self._root: EntityManager
        self._db: DB
        self._schema: Schema | None
        self._dependencies: set[str]
        self._registry: EntityRegistry
        if parent:
            self._root = parent._root or parent
            self._db = db or parent._db
            self._schema = schema or parent._schema
            self._dependencies = (
                dependencies if dependencies is not None else (parent._dependencies)
            )
            self._registry = registry or parent._registry
        else:
            if not db:
                raise ValueError("db is required")
            if not registry:
                raise ValueError("registry is required")
            self._db = db
            self._root = self
            self._schema = schema
            self._registry = registry
            self._dependencies = dependencies or set()

        self.Table = self._wrap_entity_factory(Table.create)
        self.Index = self._wrap_entity_factory(Index.create)
        self.Function = self._wrap_entity_factory(Function.create)
        self.Trigger = self._wrap_entity_factory(Trigger.create)
        self.Schema = self._wrap_entity_factory(Schema.create)

    def _wrap_entity_factory[**P, E: DBEntity](
        self,
        entity_factory: Callable[Concatenate[Self, P], E],
    ) -> Callable[P, E]:
        @functools.wraps(entity_factory)
        def factory(*args, **kwargs) -> E:
            entity = entity_factory(self, *args, **kwargs)
            self._registry.register(entity)
            return entity

        return factory

    def update_refs(self, entity: DBEntity):
        self._registry.update_node(entity)

    @property
    def db(self) -> DB:
        return self._db

    @property
    def dependency_refs(self) -> set[str]:
        return self._dependencies

    @property
    def schema(self) -> Schema | None:
        return self._schema

    @property
    def root(self) -> "EntityManager":
        return self._root

    @property
    def registry(self) -> EntityRegistry:
        return self._registry

    @classmethod
    def create_root(
        cls, db: DB, registry: EntityRegistry | None = None
    ) -> "EntityManager":
        return cls(db=db, registry=registry or EntityRegistry())

    def after(self, *entities: DBEntity) -> "EntityManager":
        """
        Create a new entity manager with the given entities as dependencies.
        Previous dependencies NOT preserved.

        Usage::

            # my_other_table will have my_table as a dependency
            manager.after(manager.Table("my_table")).Table("my_other_table")

            # to clear dependencies
            manager_with_deps = manager.after(manager.Table("my_table"))
            manager_without_deps = manager_with_deps.after()

        Returns:
            A NEW entity manager
        """
        return EntityManager(
            parent=self,
            dependencies={entity.ref for entity in entities},
        )

    def with_schema(self, schema: Schema) -> "EntityManager":
        """
        Create a new entity manager with the given schema. Dependencies are preserved.

        Usage::

            manager.with_schema(manager.Schema("my_schema"))

        Returns:
            A NEW entity manager
        """
        return EntityManager(parent=self, schema=schema)
