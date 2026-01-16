from abc import ABC, abstractmethod
from enum import StrEnum
from rawmigrate.entity import DBEntity


class NodeMutationType(StrEnum):
    CREATE = "CREATE"
    DROP = "DROP"
    RECREATE = "RECREATE"
    ALTER = "ALTER"
    UNCHANGED = "UNCHANGED"


class Comparator[T: DBEntity](ABC):
    def __init__(self, old: T, new: T):
        self.old = old
        self.new = new

    @property
    @abstractmethod
    def mutation_type(self) -> NodeMutationType: ...


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
