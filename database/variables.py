from collections import UserList

class Singleton(UserList): ...

class Params:
    def __init__(self, **params):
        for key, value in params.items():
            self.__setattr__(key, value)


class Const: ...

class Var:
    def index(self, database=None, table=None, params=Params()):
        self.database = self.database or database
        self.table = self.table or table

        indexes = params.indexes
        column = params.column
        index = params.index

        def register(field):
            if type(field) != Singleton:
                if field not in indexes[column]:
                    indexes[column][field] = {}

                indexes[column][field][index] = index
            else:
                for value in field:
                    register(value)

        # if self.stored:
        #     prev = self.prev
        #     field = self.compute()

        #     if field != prev :
        #         del indexes[column][prev][index]
        #         if not indexes[column][prev]:
        #             del indexes[column][prev]
        # else:
        #    field = self.compute()

        field = self.compute()

        register(field)
        
        self.reference()

        if self.references:
            references = self.database.tables[self.table]['references']

            for col, rows in self.references.items():
                for row in rows:
                    if row not in references[col]:
                        references[col][row] = {}

                    if column not in references[col][row]:
                        references[col][row][column] = []

                    if index not in references[col][row][column]:
                        references[col][row][column].append(index)

    def unindex(self, database=None, table=None, params=Params()):
        self.database = self.database or database
        self.table = self.table or table

        indexes = params.indexes
        column = params.column
        index = params.index

        def unregister(field):
            if type(field) != Singleton:
                del indexes[column][field][index]
                if not indexes[column][field]:
                    del indexes[column][field]
            else:
                for value in field:
                    if isinstance(value, Var):
                        value = value.compute(self.database, self.table)

                    unregister(value)

        field = self.retrieve()

        unregister(field)

    def reference(self, database=None, table=None):
        ...

    def _updatereferences(self, references={}):
        for column, indexes in references.items():
            if column not in self.references:
                self.references[column] = []

            for index in indexes:
                if index not in self.references[column]:
                    self.references[column].append(index)

    def retrieve(self, database=None, table=None):
        self.database = self.database or database
        self.table = self.table or table

        if self.stored:
            return self.prev
        else:
            return self.compute()
                    

class Escape(Var, Const):
    def __init__(self, variable):
        self.database = None
        self.table = None

        self.variable = variable
        self.references = {}
        self.stored = False

    def __len__(self): return 1
    
    def compute(self, database=None, table=None):
        return self.variable

class Null(Var):
    class null:...

    NULL = null()

    def __init__(self):
        self.database = None
        self.table = None

        self.value = Null.NULL
        self.references = {}
        self.stored = False

    def __len__(self): return 0

    def process(self, database=None, table=None, params=Params()):
        self.database = self.database or database
        self.table = self.table or table

        column = params.column

        return self.database._select(self.table, column, self.retrieve())
    
    def compute(self, database=None, table=None):
        return self.value

class Any(Var):
    def __init__(self, values: list, database=None, table=None):
        self.database = database
        self.table = table

        self.values = values
        self.references = {}
        self.stored = False
        self.prev = None

    def __len__(self): return 1

    def reference(self, database=None, table=None):
        self.database = self.database or database
        self.table = self.table or table

        self.references = {}

        if not isinstance(self.values, Var):
            for value in self.values:
                if isinstance(value, Var):
                    value.reference(self.database, self.table)
                    self._updatereferences(value.references)
        else:
            self.values.reference(self.database, self.table)
            self._updatereferences(self.values.references)

    def process(self, database=None, table=None, params=Params()):
        self.database = self.database or database
        self.table = self.table or table

        column = params.column

        results = []

        values = self.retrieve()

        for value in values:
            results.append(database._select(table, column, value))

        return list(set(results[0]).union(*results[1:]))
    
    def compute(self, database=None, table=None):
        self.database = self.database or database
        self.table = self.table or table

        if not isinstance(self.values, Var):
            values = []
            for value in self.values:
                if isinstance(value, Var):
                    value = value.compute(self.database, self.table)
                values.append(value)
        else:
            values = self.values.compute(self.database, self.table)

        curr = Singleton(values)
        self.prev = curr
        self.stored = True

        return curr

class Values(Var):
    def __init__(self, column, database=None, table=None): # Find better default for column!
        self.database = database
        self.table = table

        self.column = column
        self.references = {}
        self.stored = False
        self.prev = None

    def __len__(self): return len(self.retrieve())

    def reference(self, database=None, table=None):
        self.database = self.database or database
        self.table = self.table or table

        self.references = {}

        if not isinstance(self.column, Var):
            self._updatereferences({self.column: ['*']})
        else:
            self.column.reference(self.database, self.table)
            self._updatereferences(self.column.references)

    def process(self, database=None, table=None, params=Params()):
        self.database = self.database or database
        self.table = self.table or table

        column = params.column

        return database._select(self.table, column, self.retrieve()) # Check for similar here instead!
    
    def compute(self, database=None, table=None):
        self.database = self.database or database
        self.table = self.table or table

        if not isinstance(self.column, Var):
            column = self.column
        else:
            column = self.column.compute(self.database, self.table)

        curr = list(self.database.tables[self.table]['indexes'][column].keys())
        self.prev = curr
        self.stored = True

        return curr
    
class Field(Var):
    def __init__(self, row, column, database=None, table=None): # Find better default for column!
        self.database = database
        self.table = table

        self.row = row
        self.column = column
        self.references = {}
        self.stored = False
        self.prev = None

    def reference(self, database=None, table=None):
        self.database = self.database or database
        self.table = self.table or table

        self.references = {}

        if not isinstance(self.row, Var) and not isinstance(self.column, Var):
            key = self.database.tables[self.table]['primarykey']
            index = self.database._select(self.table, key, self.row)[0]
            self._updatereferences({self.column: [index]})
        else:
            if not isinstance(self.row, Var):
                row = self.row
            else:
                row = self.row.retrieve(self.database, self.table)

            if not isinstance(self.column, Var):
                column = self.column
            else:
                column = self.column.retrieve(self.database, self.table)

            key = self.database.tables[self.table]['primarykey']
            index = self.database._select(self.table, key, row)[0]
            self._updatereferences({column: [index]})

            if isinstance(self.row, Var):
                self.row.reference(self.database, self.table)
                self._updatereferences(self.row.references)

            if isinstance(self.column, Var):
                self.column.reference(self.database, self.table)
                self._updatereferences(self.column.references)

    def process(self, database=None, table=None, params=Params()):
        self.database = self.database or database
        self.table = self.table or table

        column = params.column

        return database._select(self.table, column, self.retrieve())
    
    def compute(self, database=None, table=None):
        self.database = self.database or database
        self.table = self.table or table

        if not isinstance(self.row, Var):
            row = self.row
        else:
            row = self.row.compute(self.database, self.table)

        if not isinstance(self.column, Var):
            column = self.column
        else:
            column = self.column.compute(self.database, self.table)

        Table = self.database.tables[self.table]
        key = Table['primarykey']
        index = list(Table['indexes'][key][row].keys())[0]
        entry = Table['entries'][index]
        offset = Table['columns'][column]

        curr = entry[offset]
        self.prev = curr
        self.stored = True

        return curr

class Formula(Var):
    def __init__(self, function, *orderedparameters, database=None, table=None, **namedparameters):
        self.database = database
        self.table = table

        self.function = function
        self.orderedparameters = orderedparameters
        self.namedparameters = namedparameters
        self.references = {}
        self.stored = False
        self.prev = None

    def reference(self, database=None, table=None):
        self.database = self.database or database
        self.table = self.table or table

        self.references = {}

        if isinstance(self.function, Var):
            self.function.reference(self.database, self.table)
            self._updatereferences(self.function.references)

        for parameter in self.orderedparameters:
            if isinstance(parameter, Var):
                parameter.reference(self.database, self.table)
                self._updatereferences(parameter.references)

        for parameter in self.namedparameters.values():
            if isinstance(parameter, Var):
                parameter.reference(self.database, self.table)
                self._updatereferences(parameter.references)

    def process(self, database=None, table=None, params=Params()):
        self.database = self.database or database
        self.table = self.table or table

        column = params.column

        return database._select(self.table, column, self.retrieve()) # Check for similar here instead!
    
    def compute(self, database=None, table=None):
        self.database = self.database or database
        self.table = self.table or table

        if not isinstance(self.function, Var):
            function = self.function
        else:
            function = self.function.retrieve(self.database, self.table)

        orderedparameters = list(self.orderedparameters)

        for n, value in enumerate(self.orderedparameters):
            if isinstance(value, Var):
                orderedparameters[n] = value.compute(self.database, self.table)

        namedparameters = dict(self.namedparameters)

        for param, value in self.namedparameters.items():
            if isinstance(value, Var):
                namedparameters[param] = value.compute(self.database, self.table)

        curr = function(*orderedparameters, **namedparameters)
        self.prev = curr
        self.stored = True

        return curr


class Numbers:
    class max(Var):
        def __init__(self, column, database=None, table=None):
            self.database = database
            self.table = table

            self.column = column
            self.references = {}
            self.stored = False
            self.prev = None

        def reference(self, database=None, table=None):
            self.database = self.database or database
            self.table = self.table or table

            self.references = {}

            if not isinstance(self.column, Var):
                self._updatereferences({self.column: ['*']})
            else:
                self.column.reference(self.database, self.table)
                self._updatereferences(self.column.references)

        def process(self, database=None, table=None, params=Params()):
            self.database = self.database or database
            self.table = self.table or table

            column = params.column

            return database._select(self.table, column, self.retrieve())

        def compute(self, database=None, table=None):
            self.database = self.database or database
            self.table = self.table or table
            
            if not isinstance(self.column, Var):
                column = self.column
            else:
                column = self.column.compute(self.database, self.table)
        
            curr = max(self.database.tables[self.table]['indexes'][column])
            self.prev = curr
            self.stored = True

            return curr

    class min(Var):
        def __init__(self, column, database=None, table=None):
            self.database = database
            self.table = table

            self.column = column
            self.references = {}
            self.stored = False
            self.prev = None

        def reference(self, database=None, table=None):
            self.database = self.database or database
            self.table = self.table or table

            self.references = {}

            if not isinstance(self.column, Var):
                self._updatereferences({self.column: ['*']})
            else:
                self.column.reference(self.database, self.table)
                self._updatereferences(self.column.references)

        def process(self, database=None, table=None, params=Params()):
            self.database = self.database or database
            self.table = self.table or table

            column = params.column

            return database._select(self.table, column, self.retrieve())

        def compute(self, database=None, table=None):
            self.database = self.database or database
            self.table = self.table or table
            
            if not isinstance(self.column, Var):
                column = self.column
            else:
                column = self.column.compute(self.database, self.table)

            curr = min(self.database.tables[self.table]['indexes'][column])
            self.prev = curr
            self.stored = True

            return curr

    class sum(Var):
        def __init__(self, column, database=None, table=None):
            self.database = database
            self.table = table

            self.column = column
            self.references = {}
            self.stored = False
            self.prev = None

        def reference(self, database=None, table=None):
            self.database = self.database or database
            self.table = self.table or table

            self.references = {}

            if not isinstance(self.column, Var):
                self._updatereferences({self.column: ['*']})
            else:
                self.column.reference(self.database, self.table)
                self._updatereferences(self.column.references)

        def process(self, database=None, table=None, params=Params()):
            self.database = self.database or database
            self.table = self.table or table

            column = params.column

            return database._select(self.table, column, self.retrieve())

        def compute(self, database=None, table=None):
            self.database = self.database or database
            self.table = self.table or table
            
            if not isinstance(self.column, Var):
                column = self.column
            else:
                column = self.column.compute(self.database, self.table)

            curr = sum(self.database.tables[self.table]['indexes'][column])
            self.prev = curr
            self.stored = True

            return curr


class Strings:
    ...

class Dates:
    ...
