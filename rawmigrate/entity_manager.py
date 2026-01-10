import functools
from typing import TYPE_CHECKING, Callable, Concatenate, Self
from rawmigrate.core import DB

from rawmigrate.entities.table import Table
from rawmigrate.entities.index import Index
from rawmigrate.entities.function import Function
from rawmigrate.entities.trigger import Trigger
from rawmigrate.entities.schema import Schema

if TYPE_CHECKING:
    from rawmigrate.entity import DBEntity


class EntityManager:
    def __init__(
        self,
        db: DB,
        schema: Schema | None = None,
        parent: "EntityManager | None" = None,
        dependencies: set[str] = set(),
    ):
        self._db = db
        self._parent = parent
        self._schema = schema
        self._dependencies = dependencies

        self._root: EntityManager = (parent._root or parent) if parent else self

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
            self._db.entity_storage[entity.ref] = entity
            return entity

        return factory

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

    @classmethod
    def create_root(cls, db: DB, schema: Schema | None = None) -> "EntityManager":
        return cls(
            db=db,
            schema=schema,
            dependencies={schema.ref} if schema else set(),
        )

    def after(self, *entities: DBEntity) -> "EntityManager":
        return EntityManager(
            db=self._db,
            schema=self._schema,
            parent=self,
            dependencies={entity.ref for entity in entities},
        )

    def with_schema(self, schema: Schema) -> "EntityManager":
        return EntityManager(
            db=self._db,
            schema=schema,
            parent=self,
            dependencies=self._dependencies,
        )
