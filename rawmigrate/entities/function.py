from enum import StrEnum
from typing import TYPE_CHECKING, OrderedDict, override

from rawmigrate.core import SqlText, SqlTextLike
from rawmigrate.entity import DBEntity
from rawmigrate.core import SqlIdentifier

if TYPE_CHECKING:
    from rawmigrate.entity_manager import EntityManager


class Function(SqlIdentifier, DBEntity):
    def __init__(
        self,
        manager: "EntityManager",
        entity_ref: str,
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
        DBEntity.__init__(self, manager, entity_ref)
        SqlIdentifier.__init__(self, manager.db.syntax, [name], [self.ref])

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
        entity_ref = _entity_ref or cls.create_ref(_manager, f"{_name}.{args_hash}")

        return cls(
            manager=_manager,
            entity_ref=entity_ref,
            name=_name,
            args=cleaned_args,
            returns=SqlText(_manager.db.syntax, returns),
            language=language,
            body=SqlText(_manager.db.syntax, body),
        )

    def _infer_dependency_refs(self) -> set[str]:
        deps = self.returns.references | self.body.references
        if not deps and self._manager.schema:
            deps.add(self._manager.schema.ref)
        return deps

    @override
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "returns": self.returns.sql,
            "language": self.language,
            "body": self.body.sql,
            "args": [arg.sql for arg in self.args.values()],
        }

    @override
    @classmethod
    def from_dict(cls, manager: "EntityManager", entity_ref: str, data: dict):
        return cls(
            manager=manager,
            entity_ref=entity_ref,
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


class NodeMutationType(StrEnum):
    CREATE = "CREATE"
    DROP = "DROP"
    RECREATE = "RECREATE"
    ALTER = "ALTER"
    IDLE = "IDLE"


"""
walking through the nodes....
1 - table 'user', diff, nothing changed
    -> IDLE

1 -> 2 - table 'subscription', diff, added a new field
    1 was IDLE, so nothing more to do
    -> ALTER

2 -> 3 - function 'handle_new_subscription', diff, added a new argument, so we need to recreate
    2 was ALTER, but this doesn't affect the function anyway
    -> RECREATE

3 -> 4 - trigger 'handle_new_subscription_trigger', diff, nothing changed really
    3 was RECREATE, so we need to RECREATE here as well
    -> RECREATE

5 - table 'useless', removed, so we need to DROP
    -> DROP



for node in dependency_order:
    node.intrinsic = compute_change(old, new)  # what changed in THIS node
    
    # propagate: if any is DROP/RECREATE, must RECREATE
    if any(dep.final in (DROP, RECREATE) for dep in node.dependencies):
        if node.intrinsic in (IDLE, ALTER):
            node.final = RECREATE  # forced by parent
        else:
            node.final = node.intrinsic
    else:
        node.final = node.intrinsic



operations = []

# 1: DROP in reverse dependency order (dependents first)
for node in reversed(dependency_order):
    if node.final in (DROP, RECREATE):
        operations.append(node.to_drop_sql())

# 2: CREATE/ALTER in dependency order (dependencies first)
for node in dependency_order:
    match node.final:
        case CREATE | RECREATE:
            operations.append(node.to_create_sql())
        case ALTER:
            operations.extend(node.to_alter_sql())
"""
