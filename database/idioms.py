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