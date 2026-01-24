from dataclasses import dataclass
import functools
from typing import TYPE_CHECKING, Callable, Concatenate, Iterable, Self
from rawmigrate.core import DB
import graphlib

from rawmigrate.entities.table import Table
from rawmigrate.entities.index import Index
from rawmigrate.entities.function import Function
from rawmigrate.entities.trigger import Trigger
from rawmigrate.entities.schema import Schema
from rawmigrate.entity import EntityBundle

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
        if not isinstance(other, EntityNode):
            return False
        return self.entity.__eq__(other.entity)


class EntityRegistry:
    def __init__(self):
        self._registry: dict[str, EntityNode] = dict()

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

    def iter_topological(self) -> Iterable[EntityNode]:
        """
        Return topologically sorted nodes from the registry.
        """
        return graphlib.TopologicalSorter(
            {node: node.dependencies for node in self._registry.values()}
        ).static_order()

    def iter_branches(self, head: str) -> Iterable[tuple[EntityNode, EntityNode]]:
        """
        Iterate over the branches of the tree, moving towards the given head.

        Returns:
            An iterable of tuples, where the first element is the parent node and the second element is the child node.
        """

        def _iter(visited: set[EntityNode], node: EntityNode):
            for dependant in node.dependants:
                if dependant not in visited:
                    visited.add(dependant)
                    yield from _iter(visited, dependant)
                    yield (node, dependant)

        return _iter(set(), self._registry[head])

    def get_node(self, ref: str, allow_none: bool = False) -> EntityNode | None:
        try:
            return self._registry[ref]
        except KeyError:
            if not allow_none:
                raise ValueError(f"Node for Entity {ref} not found")
            return None

    def get_entity(self, ref: str, allow_none: bool = False) -> DBEntity | None:
        try:
            return self._registry[ref].entity
        except KeyError:
            if not allow_none:
                raise ValueError(f"Entity {ref} not found")
            return None

    def __contains__(self, ref: str) -> bool:
        return ref in self._registry


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

        self._entity_classes: dict[str, type[DBEntity]] = {
            "Table": Table,
            "Index": Index,
            "Function": Function,
            "Trigger": Trigger,
            "Schema": Schema,
        }

    def _wrap_entity_factory[**P, E: DBEntity](
        self,
        entity_factory: Callable[Concatenate[Self, P], EntityBundle[E]],
    ) -> Callable[P, E]:
        @functools.wraps(entity_factory)
        def factory(*args, **kwargs) -> E:
            bundle = entity_factory(self, *args, **kwargs)
            for entity in bundle.all:
                if entity.ref in self._registry:
                    raise ValueError(f"Entity {entity.ref} already registered")
            for entity in bundle.all:
                self._registry.register(entity)
            return bundle.main

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

    def export_dicts(self) -> list[dict]:
        return [
            node.entity.to_dict()
            | {
                "__type__": node.entity.__class__.__name__,
            }
            for node in self.registry.iter_topological()
            if node.entity.manage_export
        ]

    def import_dicts(self, data: list[dict]):
        for entity_data in data:
            entity_class = self._entity_classes[entity_data["__type__"]]
            bundle = entity_class.from_dict(self, entity_data)
            for entity in bundle.all:
                self._registry.register(entity)
