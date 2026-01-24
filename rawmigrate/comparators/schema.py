from rawmigrate.comparator import Comparator, NodeMutationType
from rawmigrate.entities.schema import Schema


class SchemaComparator(Comparator[Schema]):
    def _compute_mutation_type(self) -> NodeMutationType:
        if self.old is None:
            return NodeMutationType.CREATE
        if self.old.name != self.new.name:
            return NodeMutationType.ALTER
        return NodeMutationType.UNCHANGED
