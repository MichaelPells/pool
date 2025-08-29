import threading
import copy

from database.variables import *
from database.idioms import *
from database.operators import *

class Result:
    def __init__(self, rows=[], database=None):
        self.rows = list(rows)
        self.database = database

        self.count = len(self.rows)

    def __len__(self):
        return self.count

    def get(self, row: list | Any = None, column: list | set | Any = Null(), table = None):
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
        self.backup = {}
        self.recenttables = []

    def _buildindex(self, table, rows=Result(), columns=[]):
        Table = self.tables[table]
        columns = {column: Table['columns'][column] for column in columns} or Table['columns']
        entries = Table['entries']
        indexes = Table['indexes']
        references = Table['references']

        for column in columns:
            if column not in indexes:
                indexes[column] = {}

        rows = rows.rows or Table['entries'].keys()

        wildcardcolumns = []
        resetfields = []

        for index in rows:
            for column, offset in columns.items():
                row = entries[index]
                field = row[offset]

                if not isinstance(field, Var):
                    if field not in indexes[column]:
                        indexes[column][field] = {}

                    indexes[column][field][index] = index
                else:
                    field.index(self, table, Params(indexes=indexes, column=column, index=index))

                    if field.references:
                        resetfields.append(field)
                
                # Rebuild index for its dependent variables in references
                if index in references[column]:
                    cols = references[column][index]

                    for col, rs in list(cols.items()):
                        self._clearindex(table, Result(rs, self), [col])
                        self._buildindex(table, Result(rs, self), [col])

                if '*' in references[column]:
                    if column not in wildcardcolumns:
                        wildcardcolumns.append(column)

        for column in wildcardcolumns:
            cols = references[column]['*']

            for col, rs in list(cols.items()):
                self._clearindex(table, Result(rs, self), [col])
                self._buildindex(table, Result(rs, self), [col])

        for field in resetfields:
            field.cycle = 0

    def _clearindex(self, table, rows=Result(), columns=[]):
        Table = self.tables[table]
        columns = {column: Table['columns'][column] for column in columns} or Table['columns']
        entries = Table['entries']
        indexes = Table['indexes']

        rows = rows.rows or Table['entries'].keys()

        for index in rows:
            for column, offset in columns.items():
                row = entries[index]
                field = row[offset]

                if not isinstance(field, Var):
                    del Table['indexes'][column][field][index]
                    if not Table['indexes'][column][field]:
                        del Table['indexes'][column][field]
                else:
                    field.unindex(self, table, Params(indexes=indexes, column=column, index=index))

    def _select(self, table, column=None, value=None): # What should really be the defaults here?
        Column = self.tables[table]['indexes'][column]

        if not isinstance(value, Var):
            if value not in Column:
                return []
            else:
                return list(Column[value].keys())
        elif isinstance(value, Const):
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
        
        if isinstance(query, Gate):
            results = []

            for operand in query.operands:
                if type(operand) == dict:
                    queries = operand

                    for column, value in queries.items():
                        results.append(self._select(table=table, column=column, value=value))
                else:
                    results.append(self._selector(table, operand))

            return query.process(results, table, self)

    def create(self, table, columns=[], entries=[], primarykey=None): # What happens when entries contain dependent variables?
        with self.lock:
            self.recenttables = [table]

            references = {column: {} for column in columns}
            columns = {column: offset for offset, column in enumerate(columns)}
            entries = {(index + 1): entry for index, entry in enumerate(entries)}
            count = len(entries)

            self.tables[table] = {
                'columns': columns,
                'entries': entries,
                'references': references,
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

            self.recenttables = [table]

            if rows == None:
                rows = Table['entries'].keys()
            else:
                rows = self._selector(table, rows)
    
            columns = {column: Table['columns'][column] for column in record}

            self._clearindex(table, Result(rows, self), columns)

            oldvalues = {}

            for column in record:
                oldvalues[column] = {}
                offset = Table['columns'][column]

                for index in rows:
                    oldvalues[column][index] = copy.copy(Table['entries'][index])[offset]

            def undo():
                self._clearindex(table, Result(rows, self), columns)

                for column in oldvalues:
                    offset = Table['columns'][column]

                    for index in rows:
                        Table['entries'][index][offset] = oldvalues[column][index]

                self._buildindex(table, Result(rows, self), columns)

            self.backup[table] = undo

            for column, value in record.items():
                offset = Table['columns'][column]

                for index in rows:
                    if not isinstance(value, Idiom):
                        Table['entries'][index][offset] = value
                    else:
                        Table['entries'][index][offset] = value.decode(locals())

            try:
                self._buildindex(table, Result(rows, self), columns)
            except Exception:
                self.undo()

                # raise

    def insert(self, table, entries):
        with self.lock:
            Table = self.tables[table]
            start = Table['nextindex']
            stop = start + len(entries)
            rows = range(start, stop)
            entries = {(start + index): entry for index, entry in enumerate(entries)}

            Table['entries'].update(entries)
            Table['count'] += len(entries)
            Table['nextindex'] = stop

            self._buildindex(table, Result(rows, self))

    def delete(self, table):
        with self.lock:
            Table = self.tables[table]

            self.recenttables = [table]

            oldTable = copy.copy(Table)

            def undo():
                self.tables[table] = oldTable

            self.backup[table] = undo

            del self.tables[table]

    def remove(self, table, rows=None):
        with self.lock:
            Table = self.tables[table]

            if rows == None:
                rows = Table['entries'].keys()
            else:
                rows = self._selector(table, rows)

            self._clearindex(table, Result(rows, self))

            for index in rows:
                del Table['entries'][index]

            Table['count'] -= len(rows)

    def undo(self):
        for table in self.recenttables:
            if table not in self.backup: # Just created
                del self.tables[table]
            else:
                self.backup[table]()
                del self.backup[table]
        
        self.recenttables = []
