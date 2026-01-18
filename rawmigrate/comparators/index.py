from rawmigrate.comparator import Comparator, NodeMutationType
from rawmigrate.entities.index import Index


class IndexComparator(Comparator[Index]):
    def _compute_mutation_type(self) -> NodeMutationType:
        return NodeMutationType.UNCHANGED