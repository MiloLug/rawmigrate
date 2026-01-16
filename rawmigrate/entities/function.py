from typing import TYPE_CHECKING, OrderedDict, override

from rawmigrate.core import SqlText, SqlTextLike
from rawmigrate.entity import EntityBundle, SchemaDependantEntity
from rawmigrate.core import SqlIdentifier

if TYPE_CHECKING:
    from rawmigrate.entity import DBEntity
    from rawmigrate.entity_manager import EntityManager


class Function(SqlIdentifier, SchemaDependantEntity):
    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
        schema: "DBEntity | None",
        dependencies: set[str] | None,
        name: str,
        args: OrderedDict[str, SqlText],
        returns: SqlText,
        language: str,
        body: SqlText,
    ):
        self.name = name
        self.returns = returns
        self.language = language
        self.body = body
        self.args = args
        SchemaDependantEntity.__init__(self, manager, entity_ref, schema, dependencies)
        SqlIdentifier.__init__(self, manager.db.syntax, [name], [entity_ref])

    @classmethod
    def create(
        cls,
        _manager: "EntityManager",
        _name: str,
        args: OrderedDict[str, SqlTextLike] | None = None,
        _entity_ref: str = "",
        *,
        returns: SqlTextLike,
        language: str = "plpgsql",
        body: SqlTextLike,
    ):
        cleaned_args = OrderedDict(
            {
                arg_name: SqlText(_manager.db.syntax, arg_value)
                for arg_name, arg_value in (args or OrderedDict()).items()
            }
        )
        args_hash = hash(tuple(cleaned_args.values()))
        entity_ref = _entity_ref or cls.create_ref(
            f"{_name}.{args_hash}", schema=_manager.schema
        )

        return EntityBundle(
            cls(
                manager=_manager,
                entity_ref=entity_ref,
                schema=_manager.schema,
                dependencies=_manager.dependency_refs,
                name=_name,
                args=cleaned_args,
                returns=SqlText(_manager.db.syntax, returns),
                language=language,
                body=SqlText(_manager.db.syntax, body),
            )
        )

    def _infer_dependency_refs(self) -> set[str]:
        return self.returns.references | self.body.references

    @override
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "schema": self._schema.ref if self._schema else None,
            "ref": self.ref,
            "returns": self.returns.sql,
            "language": self.language,
            "body": self.body.sql,
            "args": {
                arg_name: arg_value.sql for arg_name, arg_value in self.args.items()
            },
            "dependencies": list(self.dependency_refs),
        }

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", data: dict):
        return EntityBundle(
            cls(
                manager=manager,
                entity_ref=data["ref"],
                schema=manager.registry.get_entity(data["schema"])
                if data["schema"]
                else None,
                dependencies=set(data["dependencies"]),
                name=data["name"],
                args=OrderedDict(
                    {
                        arg_name: SqlText(manager.db.syntax, arg_value)
                        for arg_name, arg_value in data["args"].items()
                    }
                ),
                returns=SqlText(manager.db.syntax, data["returns"]),
                language=data["language"],
                body=SqlText(manager.db.syntax, data["body"]),
            )
        )
