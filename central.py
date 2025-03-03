from pool import Pool, Task
import threading

class Network:
    def __init__(self, app):
        self.app = app

        self.inputs = []
        self.stages = {}
        self.running = False

        self.app(self)

    def run(self, input):
        self.inputs.append(input)

    def addstage(self, status, stage):
        if status not in self.stages:
            self.stages[status] = []

        self.stages[status].append(stage)

    def start(self):
        self.running = True

        threading.Thread(target=self.run)

    def run(self):
        while self.running:
            if len(self.inputs) > 0:
                status = self.inputs[0]["status"] if "status" in self.inputs[0] else "entrypoint"
                input = self.inputs[0]["input"] if "input" in self.inputs[0] else None
                self.stages[status](input)

    def stop(self):
            self.running = False


def app(network):
    def main():
        pool = Pool()
        pool.start()

        for i in range(5):
            print(i)

        def blocker():
            for i in range(10000): pass

            # report status 'after-looper' to `network`
        pool.assign(blocker)

        def post_looper_result(result):
            print(result)
        network.addstage("after-looper", post_looper_result)

        i += 5
        print(i)

    def complete():
        print("completed")

    network.addstage("entrypoint", main)
    network.addstage("exit", complete)


project = Network(app)
project.start()