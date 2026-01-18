from rawmigrate.comparator import Comparator, NodeMutationType
from rawmigrate.entities.table import Column, Table


class ColumnComparator(Comparator[Column]):
    def _compute_mutation_type(self) -> NodeMutationType:
        return NodeMutationType.UNCHANGED


class TableComparator(Comparator[Table]):
    def _compute_mutation_type(self) -> NodeMutationType:
        return NodeMutationType.UNCHANGED