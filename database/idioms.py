class Idiom: ...


class Now(Idiom):
    def __init__(self, variable):
        self.variable = variable

    def decode(self, data):
        database = data["self"]
        table = data["table"]

        return self.variable.compute(database, table)

class This(Idiom):
    def __init__(self, function, column=None):
        self.function = function
        self.column = column

    def decode(self, data):
        Table = data["Table"]
        index = data["index"]
        offset = Table['columns'][self.column] if self.column else data["offset"]

        value = Table['entries'][index][offset]

        return self.function(value)
    
class Generator(Idiom):
    def __init__(self, init, function, controller=None, **initializers):
        self.init = init
        self.function = function
        self.controller = controller

        for name, data in initializers.items():
            self.__setattr__(name, data)

    def decode(self, data):
        if not self.controller:
            try:
                self.prev = self.function(self.prev)
            except AttributeError:
                self.prev = self.init

            return self.prev
        else:
            return self.controller(self.function, self)
