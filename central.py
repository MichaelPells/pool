from pool import Pool, Task
import threading

class Instance:
    def __init__(self, instance_id):
        self.instance_id = instance_id

        self.stages = {}

    def addstage(self, status, stage):
        self.addstages(status, [stage])

    def addstages(self, status, stages=[]):
        if status not in self.stages:
            self.stages[status] = []

        self.stages[status].extend(stages)

class Network:
    def __init__(self, app):
        self.app = app

        self.inputs = []
        self.defaultstages = []
        self.instances = {}
        self.running = False

        self.app(self)

    def do(self, input={}):
        self.inputs.append(input)

    def adddefaultstage(self, stage):
        self.defaultstages.append(stage)

    def run(self):
        instance_id = 0

        while self.running:
            if len(self.inputs) > 0:
                status = self.inputs[0]["status"] if "status" in self.inputs[0] else "entrypoint"

                if "instance" not in self.inputs[0]:
                    instance_id += 1
                    instance = self.instances[instance_id] = Instance(instance_id)

                    instance.addstages(status, self.defaultstages)

                else:
                    instance_id = self.inputs[0]["instance"]
                    instance = self.instances[instance_id]

                input = self.inputs[0]["input"] if "input" in self.inputs[0] else None

                if status == "entrypoint":
                    for stage in instance.stages[status]:
                        stage(instance, input)

                else:
                    for stage in instance.stages[status]:
                        stage(input)

                self.inputs.pop(0)

    def start(self):
        self.running = True

        threading.Thread(target=self.run).start()

    def stop(self):
            self.running = False


def app(network):
    def main(instance, _):
        pool = Pool()
        pool.start()

        for i in range(5):
            print(i)

        def blocker():
            for i in range(10000): pass

            network.do({
                "instance": instance.instance_id,
                "status": "after-looper",
                "input": "Looper Finished"
            })
        pool.assign(blocker)

        def post_looper_result(result):
            print(result)
            network.stop() # Just for safety
            pool.stop() # Just for safety
        instance.addstage("after-looper", post_looper_result)

        i += 5
        print(i)

    def complete(instance, _):
        print("completed")

    network.adddefaultstage(main)
    network.adddefaultstage(complete)


project = Network(app)
project.start()

project.do()