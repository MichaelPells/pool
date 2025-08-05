class Idiom: ...


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
    def __init__(self, initial, function, controller=None, **init):
        self.initial = initial
        self.function = function
        self.controller = controller

        for name, data in init.items():
            self.__setattr__(name, data)

    def decode(self, data):
        if not self.controller:
            try:
                self.prev = self.function(self.prev)
            except AttributeError:
                self.prev = self.initial

            return self.prev
        else:
            return self.controller(self.function, self)
