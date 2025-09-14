class Data:
    def check(data):
        return True
    
    def cast(data):
        return data

class Number(Data):
    def check(data):
        if isinstance(data, int):
            return True
        else:
            return False

    def cast(data):
        result = int(data)
        return result

class String(Data):
    ...

class List(Data):
    ...

class Object(Data):
    ...

class Date(Data):
    ...

class Any(Data):
    ...
