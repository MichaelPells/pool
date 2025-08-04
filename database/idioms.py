class Idiom: ...


class This(Idiom):
    def __init__(self, function):
        self.function = function

    def decode(self, data):
        Table = data["Table"]
        index = data["index"]
        offset = data["offset"]

        value = Table['entries'][index][offset]

        return self.function(value)