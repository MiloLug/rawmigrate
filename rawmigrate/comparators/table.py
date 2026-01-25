from rawmigrate.comparator import Comparator, NodeMutationType
from rawmigrate.entities.table import Column, Table


class ColumnComparator(Comparator[Column]):
    def _compute_mutation_type(self) -> NodeMutationType:
        if self.old is None:
            return NodeMutationType.CREATE
        if self.old.name != self.new.name:
            return NodeMutationType.ALTER
        if self.old.definition != self.new.definition:
            return NodeMutationType.ALTER
        return NodeMutationType.UNCHANGED


class TableComparator(Comparator[Table]):
    def _compute_mutation_type(self) -> NodeMutationType:
        if self.old is None:
            return NodeMutationType.CREATE
        if self.old._name != self.new._name:
            return NodeMutationType.ALTER
        return NodeMutationType.UNCHANGED
