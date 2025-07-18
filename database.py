import threading

class Params:
    def __init__(self, **params):
        for key, value in params.items():
            self.__setattr__(key, value)


class Variable:
    class Const: ...

    class Var:
        def index(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            indexes = params.indexes
            column = params.column
            index = params.index

            field = self.compute()

            if field not in indexes[column]:
                indexes[column][field] = {}

            indexes[column][field][index] = index

    class escape(Var, Const):
        def __init__(self, variable):
            self.table = None
            self.database = None

            self.variable = variable

        def __len__(self): return 1
        
        def compute(self):
            return self.variable

    class null(Var):
        class null:...

        NULL = null()

        def __init__(self):
            self.table = None
            self.database = None

            self.value = Variable.null.NULL

        def __len__(self): return 0

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return self.database._select(self.table, column, self.compute())
        
        def compute(self):
            return self.value

    class any(Var):
        def __init__(self, values: list, database=None, table=None):
            self.table = table
            self.database = database

            self.values = values

        def __len__(self): return 1

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            results = []

            values = self.compute()

            for value in values:
                results.append(database._select(table, column, value))

            return list(set(results[0]).union(*results[1:]))
        
        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database

            return self.values

    class values(Var):
        def __init__(self, column, database=None, table=None): # Find better default for column!
            self.table = table
            self.database = database

            self.column = column

        def __len__(self): return 1

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return database._select(self.table, column, self.compute()) # Check for similar here instead!
        
        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database

            return list(self.database.tables[self.table]['indexes'][self.column].keys())


class Numbers:
    Var = Variable.Var

    class max(Var):
        def __init__(self, column, database=None, table=None):
            self.table = table
            self.database = database

            self.column = column

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return database._select(self.table, column, self.compute())

        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database

            return max(self.database.tables[self.table]['indexes'][self.column])

    class min(Var):
        def __init__(self, column, database=None, table=None):
            self.table = table
            self.database = database

            self.column = column

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return database._select(self.table, column, self.compute())

        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database

            return min(self.database.tables[self.table]['indexes'][self.column])

    class sum(Var):
        def __init__(self, column, database=None, table=None):
            self.table = table
            self.database = database

            self.column = column

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return database._select(self.table, column, self.compute())

        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database

            return sum(self.database.tables[self.table]['indexes'][self.column])


class Strings:
    Var = Variable.Var


class Dates:
    Var = Variable.Var


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

    def get(self, row: list | Variable.any = None, column: list | set | Variable.any = Variable.null(), table = None):
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

                if not isinstance(field, Variable.Var):
                    if field not in indexes[column]:
                        indexes[column][field] = {}

                    indexes[column][field][index] = index
                else:
                    field.index(self, table, Params(indexes=indexes, column=column, index=index))

    def _select(self, table, column=None, value=None): # What should really be the defaults here?
        Column = self.tables[table]['indexes'][column]

        if not isinstance(value, Variable.Var):
            if value not in Column:
                return []
            else:
                return list(Column[value].keys())
        elif isinstance(value, Variable.Const):
            value = value.compute()

            if value not in Column:
                return []
            else:
                return list(Column[value].keys())
        else:
            return value.process(self, table, Params(column=column))

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