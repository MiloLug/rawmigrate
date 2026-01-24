from rawmigrate.comparator import Comparator, NodeMutationType
from rawmigrate.entities.index import Index


class IndexComparator(Comparator[Index]):
    def _compute_mutation_type(self) -> NodeMutationType:
        if self.old is None:
            return NodeMutationType.CREATE
        if self.old.on != self.new.on:
            return NodeMutationType.RECREATE
        if self.old.using != self.new.using:
            return NodeMutationType.RECREATE
        if self.old.expressions != self.new.expressions:
            return NodeMutationType.RECREATE
        return NodeMutationType.UNCHANGED
