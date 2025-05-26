from pool import Pool, Task, Input
import threading


class Null: ...

class Any:
    def __init__(self, values: list):
        self.values = values

class Rows:
    def __init__(self, rows=[]):
        self.rows = list(rows)

class Database:
    def __init__(self):
        self.lock = threading.Lock()

        self.tables = {}
        self.NULL = Null()
        self.ANY = Any

    def _buildindex(self, name, rows=Rows(), columns=[]):
        table = self.tables[name]
        columns = {column: table['columns'][column] for column in columns} or table['columns']
        entries = table['entries']
        indexes = table['indexes']

        for column in columns:
            indexes[column] = {}

        rows = rows.rows or Rows(range(len(entries))).rows

        for index in rows:
            for column, offset in columns.items():
                row = entries[index]
                field = row[offset] or self.NULL # Only works for strings for now!

                if field not in indexes[column]:
                    indexes[column][field] = {}

                indexes[column][field][index] = index

    def _select(self, name, rows=[], column=None, value=None): # What should really be the defaults here?
        if type(value) == self.ANY:
            values = value.values
        else:
            values = [value]

        column = self.tables[name]['indexes'][column]
        results = []

        for value in values:
            if value not in column:
                results.append([])

            result = column[value].keys()

            if rows:
                result = set(results).intersection(rows)

            results.append(result)
    
        return list(set(results[0]).union(*results[1:]))

    def _selector(self, name, operands):
        results = []

        for operand in operands:
            if type(operand) == dict:
                queries = operand

                for column, value in queries.items():
                    results.append(self._select(name=name, column=column, value=value))

            elif type(operand) == Rows:
                results.append(operand.rows)

        return results
    
    def AND(self, name, *operands):
        results = self._selector(name, operands)
        operation = set(results[0]).intersection(*results[1:])

        return Rows(operation)
    
    def OR(self, name, *operands):
        results = self._selector(name, operands)
        operation = set(results[0]).union(*results[1:])

        return Rows(operation)
    
    def NOT(self, name, operand):
        results = self._selector(name, [operand])
        superset = range(len(self.tables[name]['entries'])) # Would we need to create an id (and size) field or variable later?
        operation = set(superset).difference(results[0])

        return Rows(operation)

    def create(self, name, columns=[], entries=[]):
        columns = {column: offset for offset, column in enumerate(columns)}
        self.tables[name] = {
            'columns': columns,
            'entries': entries,
            'indexes': {}
        }

        self._buildindex(name)

    def read(self, name, rows=[]):
        ...

    def view(self, name, rows=Rows()):
        rows = rows.rows or Rows(range(len(self.tables[name]['entries']))).rows

        result = []

        for index in rows:
            result.append(self.tables[name]['entries'][index])

        return result

    def update(self, name, rows=Rows(), record={}):
        table = self.tables[name]

        rows = rows.rows or Rows(range(len(table['entries']))).rows
 
        columns = {}

        for column, value in record.items():
            offset = table['columns'][column]
            columns[column] = offset

            for index in rows:
                field = table['entries'][index][offset]

                del table['indexes'][column][field][index]
                if not table['indexes'][column][field]:
                    del table['indexes'][column][field]

                table['entries'][index][offset] = value

        self._buildindex(name, rows, columns)

    def insert(self, name, entries):
        start = len(self.tables[name]['entries'])
        self.tables[name]['entries'].extend(entries)
        self._buildindex(name, rows=range(start, len(self.tables[name]['entries'])))

    def delete(self, name):
        del self.tables[name]

    def remove(self, name, rows=Rows()):
        table = self.tables[name]

        rows = rows.rows or Rows(range(len(table['entries']))).rows

        for index in rows:
            for column, offset in table['columns']:
                field = table['entries'][index][offset]

                del table['indexes'][column][field][index]
                if not table['indexes'][column][field]:
                    del table['indexes'][column][field]

            table['entries'].pop(index)


class Network:
    def __init__(self, app):
        self.app = app

        self.pool = Pool()
        self.sleeper = threading.Event()
        self.ready = True

        self.nodes = {}
        self.inputs = []
        self.defaultstages = []
        self.instances = {}
        self.database = Database()
        self.running = False

        self.app(self)

    def do(self, input={}):
        while not self.ready: pass
        self.inputs.append(input)

        self.sleeper.set()
        self.sleeper.clear()

    def newnode(self, routine, name):
        self.nodes[name] = routine

    def adddefaultstage(self, stage):
        self.defaultstages.append(stage)

    def adddefaultstages(self, stages=[]):
        self.defaultstages.extend(stages)

    def run(self):
        id = 0

        while self.running:
            try:
                INPUT = self.inputs.pop(0)
            except IndexError:
                self.ready = False
                try:
                    INPUT = self.inputs.pop(0)
                except IndexError:
                    self.ready = True
                    self.sleeper.wait()
                    INPUT = self.inputs.pop(0)

            status = INPUT["status"] if "status" in INPUT else "entrypoint"

            if "instance" not in INPUT:
                id += 1
                instance = self.instances[id] = Instance(id, self)

                instance.addstages(self.defaultstages, status)

            else:
                id = INPUT["instance"]
                instance = self.instances[id]

            input = INPUT["input"] if "input" in INPUT else None

            if status == "entrypoint":
                if input is None:
                    for stage in instance.stages[status]:
                        stage(instance)

                elif type(input) == Input:
                    for stage in instance.stages[status]:
                        stage(instance, *input.args, **input.kwargs)

                else:
                    for stage in instance.stages[status]:
                        stage(instance, input)

            else:
                if input is None:
                    for stage in instance.stages[status]:
                        stage()

                elif type(input) == Input:
                    for stage in instance.stages[status]:
                        stage(*input.args, **input.kwargs)

                else:
                    for stage in instance.stages[status]:
                        stage(input)

    def start(self):
        self.running = True
        self.pool.start()

        for name, routine in self.nodes.items():
            self.pool.appoint(routine, name)

        threading.Thread(target=self.run).start()

    def stop(self):
            self.running = False
            self.pool.stop()


class Data:
    def __init__(self):
        ...


class Instance:
    def __init__(self, id, network: Network):
        self.id = id
        self.network = network

        self.stages = {}
        self.indexedstatuses = 0

        self.data = Data()

    def addstage(self, stage, status=0):
        return self.addstages([stage], status)

    def addstages(self, stages=[], status=0):
        if status == 0:
            self.indexedstatuses += 1
            status = self.indexedstatuses

        if status not in self.stages:
            self.stages[status] = []

        self.stages[status].extend(stages)

        return status

    def node(self, target, next):
        if not isinstance(target, Task):
                task = Task(target) # Create a Task object
        else:
            task = target

        if type(next) != list:
            status = next
        else:
            self.indexedstatuses += 1
            status = self.indexedstatuses
            self.addstages(next, status)

        def do():
            self.network.do({
                "instance": self.id,
                "status": status,
                "input": task.result
            })
        task.once("completed", do)

        self.network.pool.execute(task)
        return task

    def nodeon(self, node, data, next):
        if type(next) != list:
            status = next
        else:
            self.indexedstatuses += 1
            status = self.indexedstatuses
            self.addstages(next, status)

        def behaviour(operation):
            def do():
                self.network.do({
                    "instance": self.id,
                    "status": status,
                    "input": operation.result
                })
            operation.once("completed", do)

        operation = self.network.pool.assign(node, Input=Input(data), behaviour=behaviour)
        return operation
    
    def set(self, key, value):
        self.data.__setattr__(key, value)

    def get(self, key):
        return self.data.__getattribute__(key)


import time

def app(network: Network):
    def sleeper():
        time.sleep(2)
    network.newnode(sleeper, "sleeper")

    def main(instance: Instance):
        for i in range(5):
            print(i)

        def blocker():
            time.sleep(4)
            print("Helloooooooo")
            return Input(("Looper Finished",))

        def post_looper_result(result):
            print(f"Result: {result}")
            network.stop() # Just for safety

        def post_sleeper_result():
            print(f"Result2")

        instance.addstage(post_looper_result, "after-looper")
        instance.node(blocker, "after-looper")
        instance.nodeon("sleeper", {"text": "Hello"}, instance.addstage(post_sleeper_result))
        # instance.node(blocker, instance.addstage(post_looper_result))
        # instance.node(blocker, [post_looper_result])

        i += 5
        print(i)

    def complete(instance: Instance):
        print("completed")

    network.adddefaultstages([main, complete])

project = Network(app)
project.start()

project.do()
