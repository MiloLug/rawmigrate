from rawmigrate.comparator import Comparator, NodeMutationType
from rawmigrate.entities.function import Function


class FunctionComparator(Comparator[Function]):
    def _compute_mutation_type(self) -> NodeMutationType:
        if self.old is None:
            return NodeMutationType.CREATE
        if self.old.args != self.new.args:
            return NodeMutationType.ALTER
        if self.old.returns != self.new.returns:
            return NodeMutationType.ALTER
        if self.old.language != self.new.language:
            return NodeMutationType.ALTER
        if self.old.body != self.new.body:
            return NodeMutationType.ALTER
        return NodeMutationType.UNCHANGED
