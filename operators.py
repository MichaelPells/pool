class Gate:
    def __init__(self, *operands):
        self.operands = operands

class AND(Gate):
    def process(self, results, table, database):
        return set(results[0]).intersection(*results[1:])

class OR(Gate):
    def process(self, results, table, database):
        return set(results[0]).union(*results[1:])

class NOT(Gate):
    def process(self, results, table, database):
        superset = database.tables[table]['entries'].keys()
        return set(superset).difference(results[0])
