from rawmigrate.comparators.function import FunctionComparator
from rawmigrate.entities.function import Function
from rawmigrate.entity_manager import EntityRegistry


class Migrator:
    def __init__(self, old: EntityRegistry, new: EntityRegistry):
        self.old = old
        self.new = new
        self.comparators = {
            Function: FunctionComparator,
            # ...
        }

    # the rest
