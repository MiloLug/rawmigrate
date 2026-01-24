from rawmigrate.comparator import Comparator, NodeMutationType
from rawmigrate.entities.trigger import Trigger


class TriggerComparator(Comparator[Trigger]):
    def _compute_mutation_type(self) -> NodeMutationType:
        if self.old is None:
            return NodeMutationType.CREATE
        if self.old.on != self.new.on:
            return NodeMutationType.RECREATE
        if self.old.function != self.new.function:
            return NodeMutationType.RECREATE
        if self.old.procedure != self.new.procedure:
            return NodeMutationType.RECREATE
        if self.old.before != self.new.before:
            return NodeMutationType.RECREATE
        if self.old.after != self.new.after:
            return NodeMutationType.RECREATE
        if self.old.instead_of != self.new.instead_of:
            return NodeMutationType.RECREATE
        return NodeMutationType.UNCHANGED
