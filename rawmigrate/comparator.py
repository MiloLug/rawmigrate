from abc import ABC, abstractmethod
from enum import StrEnum
from rawmigrate.entity import DBEntity


class NodeMutationType(StrEnum):
    CREATE = "CREATE"
    DROP = "DROP"
    RECREATE = "RECREATE"
    ALTER = "ALTER"
    UNCHANGED = "UNCHANGED"


class Comparator[T: DBEntity](ABC):
    """
    Used to compare two versions of an entity and determine the exact mutations
    that need to be applied to the old entity to match the new one.
    """

    def __init__(self, old: T | None, new: T):
        """
        Initializes the comparator.

        Args:
            old: The old entity to compare.
            new: The new entity to compare.
        """
        self.old = old
        self.new = new
        self._mutation_type: NodeMutationType = self._compute_mutation_type()

    @abstractmethod
    def _compute_mutation_type(self) -> NodeMutationType: ...

    @property
    def mutation_type(self) -> NodeMutationType:
        return self._mutation_type
