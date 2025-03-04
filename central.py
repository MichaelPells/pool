from pool import Pool, Task
import threading

class Instance:
    def __init__(self, instance_id, network):
        self.instance_id = instance_id
        self.network = network

        self.stages = {}
        self.indexedstatuses = 0

    def addstage(self, status, stage):
        self.addstages(status, [stage])

    def addstages(self, status, stages=[]):
        if status not in self.stages:
            self.stages[status] = []

        self.stages[status].extend(stages)

    def node(self, task, next):
        if type(next) != list:
            status = next
        else:
            self.indexedstatuses += 1
            status = self.indexedstatuses
            self.addstages(status, next)

        self.network.pool.assign(target=self.network.do, kwargs={
            "input": {
                "instance": self.instance_id,
                "status": status,
                "input": task()
            }
        })


class Network:
    def __init__(self, app):
        self.app = app

        self.pool = Pool()

        self.inputs = []
        self.defaultstages = []
        self.instances = {}
        self.running = False

        self.app(self)

    def do(self, input={}):
        self.inputs.append(input)

    def adddefaultstage(self, stage):
        self.defaultstages.append(stage)

    def adddefaultstages(self, stages=[]):
        self.defaultstages.extend(stages)

    def run(self):
        instance_id = 0

        while self.running:
            if len(self.inputs) > 0:
                status = self.inputs[0]["status"] if "status" in self.inputs[0] else "entrypoint"

                if "instance" not in self.inputs[0]:
                    instance_id += 1
                    instance = self.instances[instance_id] = Instance(instance_id, self)

                    instance.addstages(status, self.defaultstages)

                else:
                    instance_id = self.inputs[0]["instance"]
                    instance = self.instances[instance_id]

                input = self.inputs[0]["input"] if "input" in self.inputs[0] else ()
                input_type = type(input)

                if status == "entrypoint":
                    if input_type == tuple or input_type == list:
                        for stage in instance.stages[status]:
                            stage(instance, *input)

                    elif input_type == dict:
                        for stage in instance.stages[status]:
                            stage(instance, **input)

                    else:
                        raise TypeError("Blah Blah")

                else:
                    if input_type == tuple or input_type == list:
                        for stage in instance.stages[status]:
                            stage(*input)

                    elif input_type == dict:
                        for stage in instance.stages[status]:
                            stage(**input)

                    else:
                        raise TypeError("Blah Blah")

                self.inputs.pop(0)

    def start(self):
        self.running = True
        self.pool.start()

        threading.Thread(target=self.run).start()

    def stop(self):
            self.running = False
            self.pool.stop()


def app(network):
    def main(instance):
        for i in range(5):
            print(i)

        def blocker():
            for i in range(10000): pass
            return ("Looper Finished",)

        def post_looper_result(result):
            print(result)
            network.stop() # Just for safety

        instance.node(blocker, [post_looper_result])

        i += 5
        print(i)

    def complete(instance):
        print("completed")

    network.adddefaultstages([main, complete])

project = Network(app)
project.start()

project.do()