from pool import Pool, Task
import threading


class Network:
    def __init__(self, app):
        self.app = app

        self.pool = Pool()
        self.sleeper = threading.Lock()
        self.ready = True

        self.inputs = []
        self.defaultstages = []
        self.instances = {}
        self.running = False

        self.app(self)

    def do(self, input={}):
        while not self.ready: pass
        self.inputs.append(input)

        try:
            self.sleeper.release()
        except: pass

    def adddefaultstage(self, stage):
        self.defaultstages.append(stage)

    def adddefaultstages(self, stages=[]):
        self.defaultstages.extend(stages)

    def run(self):
        instance_id = 0

        while self.running:
            try:
                INPUT = self.inputs.pop(0)
            except IndexError:
                self.ready = False
                try:
                    INPUT = self.inputs.pop(0)
                except IndexError:
                    self.sleeper.acquire()
                    self.ready = True
                    self.sleeper.acquire()
                    INPUT = self.inputs.pop(0)
                    try:
                        self.sleeper.release()
                    except: pass

            status = INPUT["status"] if "status" in INPUT else "entrypoint"

            if "instance" not in INPUT:
                instance_id += 1
                instance = self.instances[instance_id] = Instance(instance_id, self)

                instance.addstages(self.defaultstages, status)

            else:
                instance_id = INPUT["instance"]
                instance = self.instances[instance_id]

            input = INPUT["input"] if "input" in INPUT else ()
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

    def start(self):
        self.running = True
        self.pool.start()

        threading.Thread(target=self.run).start()

    def stop(self):
            self.running = False
            self.pool.stop()


class Instance:
    def __init__(self, instance_id, network: Network):
        self.instance_id = instance_id
        self.network = network

        self.stages = {}
        self.indexedstatuses = 0

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
                "instance": self.instance_id,
                "status": status,
                "input": task.result
            })
        task.once("completed", do)

        self.network.pool.assign(task)
        return task

import time

def app(network: Network):
    def main(instance: Instance):
        for i in range(5):
            print(i)

        def blocker():
            time.sleep(4)
            print("Helloooooooo")
            return ("Looper Finished",)

        def post_looper_result(result):
            print(f"Result: {result}")
            network.stop() # Just for safety

        instance.addstage(post_looper_result, "after-looper")
        instance.node(blocker, "after-looper")
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
