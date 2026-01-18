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
    """
    Used to compare two versions of an entity and determine the exact mutations
    that need to be applied to the old entity to match the new one.
    """

    def __init__(self, old: T, new: T, old_parents: dict[str, "Comparator"] | None = None):
        """
        Initializes the comparator.
        
        Args:
            old: The old entity to compare.
            new: The new entity to compare.
            parents: A dictionary of comparators for the dependencies of the old entity.
        """
        self.old = old
        self.new = new
        self.old_parents = old_parents or {}
        self._mutation_type: NodeMutationType | None = None
    
    @abstractmethod
    def _compute_mutation_type(self) -> NodeMutationType: ...

    @property
    def mutation_type(self) -> NodeMutationType:
        if self._mutation_type is None:
            self._mutation_type = self._compute_mutation_type()
        return self._mutation_type


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



"""
NEW ALGO:

root = dependencies
head = dependants

1. in NEW -> from root to head, collect ALTER/CREATE/RECREATE
2. in NEW -> from head to root, insert DROP of RECREATE
3. in NEW -> from root to head, insert (for each NODE):
    - furthest possible (by chain) DROP of NODE.dependants in OLD, that don't exist in NEW
    - ALTER/CREATE of NODE
4. in OLD -> from head to root, insert DROP - in case there are any stand-alone nodes left
"""