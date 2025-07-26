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
            if field not in indexes[column]:
                indexes[column][field] = {}

            indexes[column][field][index] = index

        if self.stored:
            prev = self.prev
            field = self.compute()

            if field != prev :
                del indexes[column][prev][index]
                if not indexes[column][prev]:
                    del indexes[column][prev]
        else:
           field = self.compute()

        if type(self) in [
            Any,
            Values
            ]:
            for value in field:
                register(value)
        else:
            register(field)

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
        self.stored = False # Can't Any have a reference too??

    def __len__(self): return 1

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

        if isinstance(self.values, Var):
            self.values.table = self.values.table or self.table
            self.values.database = self.values.database or self.database
            self.values = self.values.retrieve()

        return self.values

class Values(Var):
    def __init__(self, column, database=None, table=None): # Find better default for column!
        self.table = table
        self.database = database

        self.column = column
        self.references = {column: '*'} # There will be a problem if `column` is a variable!
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

        if isinstance(self.column, Var):
            self.column.table = self.column.table or self.table
            self.column.database = self.column.database or self.database
            self.column = self.column.retrieve()

        curr = list(self.database.tables[self.table]['indexes'][self.column].keys())
        self.prev = curr
        self.stored = True

        return curr
    
class Field(Var):
    def __init__(self, row, column, database=None, table=None): # Find better default for column!
        self.table = table
        self.database = database

        self.row = row
        self.column = column
        self.references = {column: '*'} #*** There will be a problem if `column` is a variable!
        self.stored = False
        self.prev = None

    def __len__(self): return 1

    def process(self, database=None, table=None, params=Params()):
        self.table = self.table or table
        self.database = self.database or database

        column = params.column

        return database._select(self.table, column, self.retrieve())
    
    def compute(self, database=None, table=None):
        self.table = self.table or table
        self.database = self.database or database

        if isinstance(self.row, Var):
            self.row.table = self.row.table or self.table
            self.row.database = self.row.database or self.database
            self.row = self.row.retrieve()

        if isinstance(self.column, Var):
            self.column.table = self.column.table or self.table
            self.column.database = self.column.database or self.database
            self.column = self.column.retrieve()

        Table = self.database.tables[self.table]
        key = Table["primarykey"]
        index = list(Table['indexes'][key][self.row].keys())[0]
        entry = Table['entries'][index]
        offset = Table['columns'][self.column]

        curr = entry[offset]
        self.prev = curr
        self.stored = True

        return curr

class Formula(Var):
    def __init__(self, function, parameters, database=None, table=None): # Find better default for column!
        self.table = table
        self.database = database

        self.function = function
        self.parameters = parameters
        self.references = {} #*** There will be a problem if `column` is a variable!
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
