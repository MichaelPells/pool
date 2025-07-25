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

                    references[col][row][column].append(index)
                    

class Escape(Var, Const):
    def __init__(self, variable):
        self.table = None
        self.database = None

        self.variable = variable
        self.references = {}

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

    def __len__(self): return 0

    def process(self, database=None, table=None, params=Params()):
        self.table = self.table or table
        self.database = self.database or database

        column = params.column

        return self.database._select(self.table, column, self.compute())
    
    def compute(self):
        return self.value

class Any(Var):
    def __init__(self, values: list, database=None, table=None):
        self.table = table
        self.database = database

        self.values = values
        self.references = {}

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

        if isinstance(self.values, Var):
            self.values.table = self.values.table or self.table
            self.values.database = self.values.database or self.database
            self.values = self.values.compute()

        return self.values

class Values(Var):
    def __init__(self, column, database=None, table=None): # Find better default for column!
        self.table = table
        self.database = database

        self.column = column
        self.references = {column: '*'}

    def __len__(self): return 1

    def process(self, database=None, table=None, params=Params()):
        self.table = self.table or table
        self.database = self.database or database

        column = params.column

        return database._select(self.table, column, self.compute()) # Check for similar here instead!
    
    def compute(self, database=None, table=None):
        self.table = self.table or table
        self.database = self.database or database

        if isinstance(self.column, Var):
            self.column.table = self.column.table or self.table
            self.column.database = self.column.database or self.database
            self.column = self.column.compute()

        return list(self.database.tables[self.table]['indexes'][self.column].keys())


class Numbers:
    class max(Var):
        def __init__(self, column, database=None, table=None):
            self.table = table
            self.database = database

            self.column = column
            self.references = {column: '*'}

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return database._select(self.table, column, self.compute())

        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database
            
            if isinstance(self.column, Var):
                self.column.table = self.column.table or self.table
                self.column.database = self.column.database or self.database
                self.column = self.column.compute()

            return max(self.database.tables[self.table]['indexes'][self.column])

    class min(Var):
        def __init__(self, column, database=None, table=None):
            self.table = table
            self.database = database

            self.column = column
            self.references = {column: '*'}

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return database._select(self.table, column, self.compute())

        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database
            
            if isinstance(self.column, Var):
                self.column.table = self.column.table or self.table
                self.column.database = self.column.database or self.database
                self.column = self.column.compute()

            return min(self.database.tables[self.table]['indexes'][self.column])

    class sum(Var):
        def __init__(self, column, database=None, table=None):
            self.table = table
            self.database = database

            self.column = column
            self.references = {column: '*'}

        def process(self, database=None, table=None, params=Params()):
            self.table = self.table or table
            self.database = self.database or database

            column = params.column

            return database._select(self.table, column, self.compute())

        def compute(self, database=None, table=None):
            self.table = self.table or table
            self.database = self.database or database
            
            if isinstance(self.column, Var):
                self.column.table = self.column.table or self.table
                self.column.database = self.column.database or self.database
                self.column = self.column.compute()

            return sum(self.database.tables[self.table]['indexes'][self.column])


class Strings:
    ...

class Dates:
    ...
