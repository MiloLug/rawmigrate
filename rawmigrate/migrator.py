from rawmigrate.comparator import Comparator, NodeMutationType
from rawmigrate.entity_manager import EntityNode, EntityRegistry
from rawmigrate.comparators import (
    FunctionComparator,
    IndexComparator,
    SchemaComparator,
    TableComparator,
    TriggerComparator,
    ColumnComparator,
)
from rawmigrate.entities import Column, Function, Index, Schema, Table, Trigger


class Migrator:
    def __init__(self, old: EntityRegistry, new: EntityRegistry):
        self.old = old
        self.new = new
        self.comparator_types = {
            Function: FunctionComparator,
            Index: IndexComparator,
            Trigger: TriggerComparator,
            Schema: SchemaComparator,
            Table: TableComparator,
            Column: ColumnComparator,
        }
        self.new_comparators: dict[str, Comparator] = {}

    def _init_comparators(self):
        for node in self.new.iter_topological():
            old = self.old.get_entity(node.entity.ref, allow_none=True)
            new = node.entity
            self.new_comparators[node.entity.ref] = self.comparator_types[
                type(node.entity)
            ](old, new)

    def test(self):
        migration = ""
        self._init_comparators()
        for new in reversed(list(self.new.iter_topological())):
            if self.new_comparators[new.entity.ref].mutation_type in (
                NodeMutationType.DROP,
                NodeMutationType.RECREATE,
            ):
                migration += f"DROP {new.entity.ref};\n"

        old_dropped: set[EntityNode] = set()
        for new in self.new.iter_topological():
            old = self.old.get_node(new.entity.ref, allow_none=True)
            if old is not None:
                for _, child in self.old.iter_branches(old.entity.ref):
                    if (
                        old_dropped.issuperset(child.dependants)
                        and child not in old_dropped
                        and child.entity.ref not in self.new
                    ):
                        old_dropped.add(child)
                        migration += f"DROP {child.entity.ref};\n"

            mutation = self.new_comparators[new.entity.ref].mutation_type
            if mutation in (NodeMutationType.CREATE, NodeMutationType.ALTER):
                migration += f"{mutation} {new.entity.ref};\n"
            elif mutation == NodeMutationType.RECREATE:
                migration += f"CREATE {new.entity.ref};\n"

        for old in reversed(list(self.old.iter_topological())):
            if old.entity.ref not in self.new_comparators and old not in old_dropped:
                migration += f"DROP {old.entity.ref};\n"

        print(migration)


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





The algorithm used to generate a migration:

root = dependencies
head = dependants

1. in NEW -> from root to head, collect ALTER/CREATE/RECREATE
2. in NEW -> from head to root, insert DROP of RECREATE
3. in NEW -> from root to head, insert (for each NODE):
    - furthest possible (by chain) DROP of NODE.dependants in OLD, that don't exist in NEW
    - ALTER/CREATE of NODE
4. in OLD -> from head to root, insert DROP - in case there are any stand-alone nodes left
"""
