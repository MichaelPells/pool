import threading


class Variable:
    class Var: ...

    class Const: ...

    class escape(Var, Const):
        def __init__(self, variable):
            self.variable = variable

        def __len__(self): return 1

        def index(self, table, database):
            ... # index variable

        def process(self, table, column, database):
            value = self.variable

            Column = database.tables[table]['indexes'][column]
        
            if value not in Column:
                    return []
            else:
                return list(Column[value].keys())
        
        def compute(self, table, database):
            ... # return variable

    class null(Var):
        def __len__(self): return 0

        def index(self, table, database):
            ...

        def process(self, table, column, database):
            return database._select(table, column, Variable.escape(self))
        
        def compute(self, table, database):
            ...

    NULL = null()

    class any(Var):
        def __init__(self, values: list):
            self.values = values

        def __len__(self): return 1

        def index(self, table, database):
            ...

        def process(self, table, column, database):
            results = []

            for value in self.values:
                results.append(database._select(table, column, value))
    
            return list(set(results[0]).union(*results[1:]))
        
        def compute(self, table, database):
            ...

    class values(Var):
        def __init__(self, column, table=None): # Find better default for column!
            self.column = column
            self.table = table

        def __len__(self): return 1

        def index(self, table, database):
            ...

        def process(self, table, column, database):
            self.table = self.table or table
            values = list(database.tables[self.table]['indexes'][self.column].keys())
    
            return database._select(table, column, Variable.any(values))
        
        def compute(self, table, database):
            ...

    class max(Var):
        def __init__(self, column):
            self.column = column

        def index(self, table, database):
            ...

        def process(self, table, column, database):
            return database._select(table, column, max(database.tables[table]['indexes'][self.column]))

        def compute(self, table, database):
            ...

    class min(Var):
        def __init__(self, column):
            self.column = column

        def index(self, table, database):
            ...

        def process(self, table, column, database):
            return database._select(table, column, min(database.tables[table]['indexes'][self.column]))

        def compute(self, table, database):
            ...

    class sum(Var):
        def __init__(self, column):
            self.column = column

        def index(self, table, database):
            ...

        def process(self, table, column, database):
            print(sum(database.tables[table]['indexes'][self.column]))
            return database._select(table, column, sum(database.tables[table]['indexes'][self.column]))

        def compute(self, table, database):
            ...

class Operator:
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


class Result:
    def __init__(self, rows=[], database=None):
        self.rows = list(rows)
        self.database = database

        self.count = len(self.rows)

    def __len__(self):
        return self.count

    def get(self, row: list | Variable.any = None, column: list | set | Variable.any = Variable.NULL, table = None):
        table = table or self.database.primarytable
        Table = self.database.tables[table]
        row = row if row != None else range(0, self.count)
        column = column or set(Table['columns'].keys())

        if type(row) == list or type(row) == range:
            entries = []
            for i in row:
                index = self.rows[i]
                entry = Table['entries'][index]
                if type(column) == list:
                    record = []
                    for col in column:
                        offset = Table['columns'][col]
                        field = entry[offset]
                        record.append(field)
                elif type(column) == set:
                    record = {}
                    for col in column:
                        offset = Table['columns'][col]
                        field = entry[offset]
                        record[col] = field
                else:
                    offset = Table['columns'][column]
                    record = entry[offset] # field

                entries.append(record)

            result = entries
        else:
            index = self.rows[row]
            entry = Table['entries'][index]

            if type(column) == list:
                result = [] # record
                for col in column:
                    offset = Table['columns'][col]
                    field = entry[offset]
                    result.append(field)
            elif type(column) == set:
                result = {} # record
                for col in column:
                    offset = Table['columns'][col]
                    field = entry[offset]
                    result[col] = field
            else:
                offset = Table['columns'][column]
                result = entry[offset] # field
        
        return result

    def sort(self, column, order):
        return self


class Database:
    def __init__(self):
        self.lock = threading.Lock()

        self.tables = {}
        self.primarytable = None

    def _buildindex(self, table, rows=Result(), columns=[]):
        Table = self.tables[table]
        columns = {column: Table['columns'][column] for column in columns} or Table['columns']
        entries = Table['entries']
        indexes = Table['indexes']

        for column in columns:
            if column not in indexes:
                indexes[column] = {}

        rows = rows.rows or Table['entries'].keys()

        for index in rows:
            for column, offset in columns.items():
                row = entries[index]
                field = row[offset]

                if field not in indexes[column]:
                    indexes[column][field] = {}

                indexes[column][field][index] = index

    def _select(self, table, column=None, value=None): # What should really be the defaults here?
        Column = self.tables[table]['indexes'][column]

        if not isinstance(value, Variable.Var):
            if value not in Column:
                return []
            else:
                return list(Column[value].keys())
        else:
            return value.process(table, column, self)

    def _selector(self, table, query):
        if type(query) == list:
            return query
        
        if type(query) == dict:
            column, value = list(query.items())[0]
            return self._select(table=table, column=column, value=value)
        
        if isinstance(query, Operator.Gate):
            results = []

            for operand in query.operands:
                if type(operand) == dict:
                    queries = operand

                    for column, value in queries.items():
                        results.append(self._select(table=table, column=column, value=value))
                else:
                    results.append(self._selector(table, operand))

            return query.process(results, table, self)

    def create(self, table, columns=[], entries=[], primarykey=None):
        with self.lock:
            columns = {column: offset for offset, column in enumerate(columns)}
            entries = {(index + 1): entry for index, entry in enumerate(entries)}
            count = len(entries)

            self.tables[table] = {
                'columns': columns,
                'entries': entries,
                'references': {},
                'indexes': {},
                'count': count,
                'nextindex': count + 1,
                'primarykey': primarykey
            }

            if not self.primarytable:
                self.primarytable = table

            self._buildindex(table)

    def read(self, table, rows=None):
        with self.lock:
            Table = self.tables[table]

            if rows == None:
                rows = Table['entries'].keys()
            else:
                rows = self._selector(table, rows)

            result = Result(rows, self)

            return result

    def view(self, table, rows=None):
        with self.lock:
            Table = self.tables[table]

            if rows == None:
                rows = Table['entries'].keys()
            else:
                rows = self._selector(table, rows)

            result = []

            for index in rows:
                result.append(Table['entries'][index])

            return result

    def update(self, table, rows=None, record={}):
        with self.lock:
            Table = self.tables[table]

            if rows == None:
                rows = Table['entries'].keys()
            else:
                rows = self._selector(table, rows)
    
            columns = {}

            for column, value in record.items():
                offset = Table['columns'][column]
                columns[column] = offset

                for index in rows:
                    field = Table['entries'][index][offset]

                    del Table['indexes'][column][field][index]
                    if not Table['indexes'][column][field]:
                        del Table['indexes'][column][field]

                    Table['entries'][index][offset] = value

            self._buildindex(table, Result(rows, self), columns)

    def insert(self, table, entries):
        with self.lock:
            Table = self.tables[table]
            start = Table['nextindex']
            stop = start + len(entries)
            entries = {(start + index): entry for index, entry in enumerate(entries)}

            Table['entries'].update(entries)
            Table['count'] += len(entries)
            Table['nextindex'] = stop

            self._buildindex(table, rows=range(start, stop))

    def delete(self, table):
        with self.lock:
            del self.tables[table]

    def remove(self, table, rows=None):
        with self.lock:
            Table = self.tables[table]

            if rows == None:
                rows = Table['entries'].keys()
            else:
                rows = self._selector(table, rows)

            for index in rows:
                for column, offset in Table['columns'].items():
                    field = Table['entries'][index][offset]

                    del Table['indexes'][column][field][index]
                    if not Table['indexes'][column][field]:
                        del Table['indexes'][column][field]

                del Table['entries'][index]

            Table['count'] -= len(rows)