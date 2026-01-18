from rawmigrate.entity_manager import EntityRegistry
from rawmigrate.comparators import FunctionComparator, IndexComparator, SchemaComparator, TableComparator, TriggerComparator, ColumnComparator
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
