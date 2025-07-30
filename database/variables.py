from collections import UserList

class Singleton(UserList): ...

class Params:
    def __init__(self, **params):
        for key, value in params.items():
            self.__setattr__(key, value)


class Const: ...

class Var:
    def index(self, database=None, table=None, params=Params()):
        self.table = self.table or table
        self.database = self.database or database

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
        self.table = self.table or table
        self.database = self.database or database

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

    def retrieve(self, database=None, table=None):
        self.table = self.table or table
        self.database = self.database or database

        if self.stored:
            return self.prev
        else:
            return self.compute()
                    

class Escape(Var, Const):
    def __init__(self, variable):
        self.table = None
        self.database = None

        self.variable = variable
        self.references = {}
        self.stored = False

    def __len__(self): return 1
    
    def compute(self):
        return self.variable

class Null(Var):
    class null:...

    NULL = null()

    def __init__(self):
        self.table = None
        self.database = None

        self.value = Null.NULL
        self.references = {}
        self.stored = False

    def __len__(self): return 0

    def process(self, database=None, table=None, params=Params()):
        self.table = self.table or table
        self.database = self.database or database

        column = params.column

        return self.database._select(self.table, column, self.retrieve())
    
    def compute(self):
        return self.value

class Any(Var):
    def __init__(self, values: list, database=None, table=None):
        self.table = table
        self.database = database

        self.values = values
        self.references = {}
        self.stored = False
        self.prev = None

    def __len__(self): return 1

    def reference(self, database=None, table=None):
        self.table = self.table or table
        self.database = self.database or database

        self.references = {}

        if not isinstance(self.values, Var):
            for value in self.values:
                if isinstance(value, Var):
                    value.reference(self.database, self.table)
                    self.references.update(value.references)
        else:
            self.values.reference(self.database, self.table)
            self.references.update(self.values.references)

    def process(self, database=None, table=None, params=Params()):
        self.table = self.table or table
        self.database = self.database or database

        column = params.column

        results = []

        values = self.retrieve()

        for value in values:
            results.append(database._select(table, column, value))

        return list(set(results[0]).union(*results[1:]))
    
    def compute(self, database=None, table=None):
        self.table = self.table or table
        self.database = self.database or database

        if not isinstance(self.values, Var):
            values = []
            for value in self.values:
                if isinstance(value, Var):
                    value = value.compute(self.database, self.table)
                values.append(value)
        else:
            self.values.table = self.values.table or self.table
            self.values.database = self.values.database or self.database
            values = self.values.compute()

        curr = Singleton(values)
        self.prev = curr
        self.stored = True

        return curr

class Values(Var):
    def __init__(self, column, database=None, table=None): # Find better default for column!
        self.table = table
        self.database = database

        self.column = column

        self.references = {}

        self.stored = False
        self.prev = None

    def __len__(self): return 1 # Should it really be 1??

    def reference(self, database=None, table=None):
        self.table = self.table or table
        self.database = self.database or database

        if not isinstance(self.column, Var):
            self.references.update({self.column: ['*']})
        else:
            self.column.reference(self.database, self.table)
            self.references.update(self.column.references)

    def process(self, database=None, table=None, params=Params()):
        self.table = self.table or table
        self.database = self.database or database

        column = params.column

        return database._select(self.table, column, self.retrieve()) # Check for similar here instead!
    
    def compute(self, database=None, table=None):
        self.table = self.table or table
        self.database = self.database or database

        if not isinstance(self.column, Var):
            column = self.column
        else:
            self.column.table = self.column.table or self.table
            self.column.database = self.column.database or self.database
            column = self.column.compute()

        curr = list(self.database.tables[self.table]['indexes'][column].keys())
        self.prev = curr
        self.stored = True

        return curr
    
class Field(Var):
    def __init__(self, row, column, database=None, table=None): # Find better default for column!
        self.table = table
        self.database = database

        self.row = row
        self.column = column
        self.references = {}
        self.stored = False
        self.prev = None

    def __len__(self): return 1

    def reference(self, database=None, table=None):
        self.table = self.table or table
        self.database = self.database or database

        self.references = {}

        if not isinstance(self.row, Var) and not isinstance(self.column, Var):
            key = self.database.tables[self.table]["primarykey"]
            index = self.database._select(self.table, key, self.row)[0]
            self.references.update({self.column: [index]})
        else:
            if not isinstance(self.row, Var):
                row = self.row
            else:
                row = self.row.retrieve()

            if not isinstance(self.column, Var):
                column = self.column
            else:
                column = self.column.retrieve()

            key = self.database.tables[self.table]["primarykey"]
            index = self.database._select(self.table, key, row)[0]
            self.references.update({column: [index]})

            if isinstance(self.row, Var):
                self.row.reference(self.database, self.table)
                self.references.update(self.row.references)

            if isinstance(self.column, Var):
                self.column.reference(self.database, self.table)
                self.references.update(self.column.references)

    def process(self, database=None, table=None, params=Params()):
        self.table = self.table or table
        self.database = self.database or database

        column = params.column

        return database._select(self.table, column, self.retrieve())
    
    def compute(self, database=None, table=None):
        self.table = self.table or table
        self.database = self.database or database

        if not isinstance(self.row, Var):
            row = self.row
        else:
            self.row.table = self.row.table or self.table
            self.row.database = self.row.database or self.database
            row = self.row.compute()

        if not isinstance(self.column, Var):
            column = self.column
        else:
            self.column.table = self.column.table or self.table
            self.column.database = self.column.database or self.database
            column = self.column.compute()

        Table = self.database.tables[self.table]
        key = Table["primarykey"]
        index = list(Table['indexes'][key][row].keys())[0]
        entry = Table['entries'][index]
        offset = Table['columns'][column]

        curr = entry[offset]
        self.prev = curr
        self.stored = True

        return curr

class Formula(Var):
    def __init__(self, function, database=None, table=None, **parameters):
        self.table = table
        self.database = database

        self.function = function
        self.parameters = parameters
        self.references = {}
        self.stored = False
        self.prev = None

    def __len__(self): return 1 # Should it really be 1??

    def process(self, database=None, table=None, params=Params()):
        self.table = self.table or table
        self.database = self.database or database

        column = params.column

        return database._select(self.table, column, self.retrieve()) # Check for similar here instead!
    
    def compute(self, database=None, table=None):
        self.table = self.table or table
        self.database = self.database or database

        if isinstance(self.function, Var):
            self.function.table = self.function.table or self.table
            self.function.database = self.function.database or self.database
            self.function = self.function.retrieve()

        # Do above for `parameters`

        curr = self.function(**self.parameters)
        self.prev = curr
        self.stored = True

        return curr


class Numbers:
    class max(Var):
        def __init__(self, column, database=None, table=None):
            self.table = table
            self.database = database

            self.column = column
            self.references = {column: '*'}
            self.stored = False
            self.prev = None

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return database._select(self.table, column, self.retrieve())

        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database
            
            if isinstance(self.column, Var):
                self.column.table = self.column.table or self.table
                self.column.database = self.column.database or self.database
                self.column = self.column.retrieve()

            curr = max(self.database.tables[self.table]['indexes'][self.column])
            self.prev = curr
            self.stored = True

            return curr

    class min(Var):
        def __init__(self, column, database=None, table=None):
            self.table = table
            self.database = database

            self.column = column
            self.references = {column: '*'}
            self.stored = False
            self.prev = None

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return database._select(self.table, column, self.retrieve())

        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database
            
            if isinstance(self.column, Var):
                self.column.table = self.column.table or self.table
                self.column.database = self.column.database or self.database
                self.column = self.column.retrieve()

            curr = min(self.database.tables[self.table]['indexes'][self.column])
            self.prev = curr
            self.stored = True

            return curr

    class sum(Var):
        def __init__(self, column, database=None, table=None):
            self.table = table
            self.database = database

            self.column = column
            self.references = {column: '*'}
            self.stored = False
            self.prev = None

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return database._select(self.table, column, self.retrieve())

        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database
            
            if isinstance(self.column, Var):
                self.column.table = self.column.table or self.table
                self.column.database = self.column.database or self.database
                self.column = self.column.retrieve()

            curr = sum(self.database.tables[self.table]['indexes'][self.column])
            self.prev = curr
            self.stored = True

            return curr


class Strings:
    ...

class Dates:
    ...
