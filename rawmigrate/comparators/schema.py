from rawmigrate.comparator import Comparator, NodeMutationType
from rawmigrate.entities.schema import Schema


class SchemaComparator(Comparator[Schema]):
    def _compute_mutation_type(self) -> NodeMutationType:
        return NodeMutationType.UNCHANGED