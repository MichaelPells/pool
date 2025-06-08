import threading


class Variable:
    class Var: ...

    class null(Var):
        def __len__(self): return 0

        def index(self, name, database):
            ...

        def process(self, name, database):
            return self
        
        def compute(self, name, database):
            ...

    NULL = null()

    class any(Var):
        def __init__(self, values: list):
            self.values = values

        def __len__(self): return 1

        def index(self, name, database):
            ...

        def process(self, name, database):
            return self.values
        
        def compute(self, name, database):
            ...

    class max(Var):
        def __init__(self, column):
            self.column = column

        def index(self, name, database):
            ...

        def process(self, name, database):
            return max(database.tables[name]['indexes'][self.column])

        def compute(self, name, database):
            ...

class Operator:
    class Gate:
        def __init__(self, *operands):
            self.operands = operands

    class AND(Gate):
        def process(self, results, name, database):
            return set(results[0]).intersection(*results[1:])

    class OR(Gate):
        def process(self, results, name, database):
            return set(results[0]).union(*results[1:])

    class NOT(Gate):
        def process(self, results, name, database):
            superset = database.tables[name]['entries'].keys()
            return set(superset).difference(results[0])


class Result:
    def __init__(self, rows=[], database=None):
        self.rows = list(rows)
        self.database = database

        self.count = len(self.rows)

    def __len__(self):
        return self.count

    def get(self, row: list | Variable.any = None, column: list | set | Variable.any = Variable.NULL, table = None):
        table = table or self.database.primarytable
        table = self.database.tables[table]
        row = row if row != None else range(0, self.count)
        column = column or set(table['columns'].keys())

        if type(row) == list or type(row) == range:
            entries = []
            for i in row:
                index = self.rows[i]
                entry = table['entries'][index]
                if type(column) == list:
                    record = []
                    for col in column:
                        offset = table['columns'][col]
                        field = entry[offset]
                        record.append(field)
                elif type(column) == set:
                    record = {}
                    for col in column:
                        offset = table['columns'][col]
                        field = entry[offset]
                        record[col] = field
                else:
                    offset = table['columns'][column]
                    record = entry[offset] # field

                entries.append(record)

            result = entries
        else:
            index = self.rows[row]
            entry = table['entries'][index]

            if type(column) == list:
                result = [] # record
                for col in column:
                    offset = table['columns'][col]
                    field = entry[offset]
                    result.append(field)
            elif type(column) == set:
                result = {} # record
                for col in column:
                    offset = table['columns'][col]
                    field = entry[offset]
                    result[col] = field
            else:
                offset = table['columns'][column]
                result = entry[offset] # field
        
        return result

    def sort(self, column, order):
        return self


class Database:
    def __init__(self):
        self.lock = threading.Lock()

        self.tables = {}
        self.primarytable = None

    def _buildindex(self, name, rows=Result(), columns=[]):
        table = self.tables[name]
        columns = {column: table['columns'][column] for column in columns} or table['columns']
        entries = table['entries']
        indexes = table['indexes']

        for column in columns:
            if column not in indexes:
                indexes[column] = {}

        rows = rows.rows or table['entries'].keys()

        for index in rows:
            for column, offset in columns.items():
                row = entries[index]
                field = row[offset]

                if field not in indexes[column]:
                    indexes[column][field] = {}

                indexes[column][field][index] = index

    def _select(self, name, column=None, value=None): # What should really be the defaults here?
        Column = self.tables[name]['indexes'][column]

        if not isinstance(value, Variable.Var):
            if value not in Column:
                return []
            else:
                return list(Column[value].keys())
        else:
            value = value.process(name, self)

            if type(value) != list:
                return self._select(name, column, value)
            else:
                values = value
                results = []

                for value in values:
                    results.append(self._select(name, column, value))
        
                return list(set(results[0]).union(*results[1:]))

    def _selector(self, name, query):
        if type(query) == list:
            return query
        
        if type(query) == dict:
            column, value = list(query.items())[0]
            return self._select(name=name, column=column, value=value)
        
        if isinstance(query, Operator.Gate):
            results = []

            for operand in query.operands:
                if type(operand) == dict:
                    queries = operand

                    for column, value in queries.items():
                        results.append(self._select(name=name, column=column, value=value))
                else:
                    results.append(self._selector(name, operand))

            return query.process(results, name, self)

    def create(self, name, columns=[], entries=[], primarykey=None):
        with self.lock:
            columns = {column: offset for offset, column in enumerate(columns)}
            entries = {(index + 1): entry for index, entry in enumerate(entries)}
            count = len(entries)

            self.tables[name] = {
                'columns': columns,
                'entries': entries,
                'references': {},
                'indexes': {},
                'count': count,
                'nextindex': count + 1,
                'primarykey': primarykey
            }

            if not self.primarytable:
                self.primarytable = name

            self._buildindex(name)

    def read(self, name, rows=None):
        with self.lock:
            table = self.tables[name]

            if rows == None:
                rows = table['entries'].keys()
            else:
                rows = self._selector(name, rows)

            result = Result(rows, self)

            return result

    def view(self, name, rows=None):
        with self.lock:
            table = self.tables[name]

            if rows == None:
                rows = table['entries'].keys()
            else:
                rows = self._selector(name, rows)

            result = []

            for index in rows:
                result.append(table['entries'][index])

            return result

    def update(self, name, rows=None, record={}):
        with self.lock:
            table = self.tables[name]

            if rows == None:
                rows = table['entries'].keys()
            else:
                rows = self._selector(name, rows)
    
            columns = {}

            for column, value in record.items():
                offset = table['columns'][column]
                columns[column] = offset

                for index in rows:
                    field = table['entries'][index][offset]

                    del table['indexes'][column][field][index]
                    if not table['indexes'][column][field]:
                        del table['indexes'][column][field]

                    table['entries'][index][offset] = value

            self._buildindex(name, Result(rows, self), columns)

    def insert(self, name, entries):
        with self.lock:
            table = self.tables[name]
            start = table['nextindex']
            stop = start + len(entries)
            entries = {(start + index): entry for index, entry in enumerate(entries)}

            table['entries'].update(entries)
            table['count'] += len(entries)
            table['nextindex'] = stop

            self._buildindex(name, rows=range(start, stop))

    def delete(self, name):
        with self.lock:
            del self.tables[name]

    def remove(self, name, rows=None):
        with self.lock:
            table = self.tables[name]

            if rows == None:
                rows = table['entries'].keys()
            else:
                rows = self._selector(name, rows)

            for index in rows:
                for column, offset in table['columns'].items():
                    field = table['entries'][index][offset]

                    del table['indexes'][column][field][index]
                    if not table['indexes'][column][field]:
                        del table['indexes'][column][field]

                del table['entries'][index]

            table['count'] -= len(rows)