class Network:
    def __init__(self, app):
        self.app = app

        self.inputs = []
        self.stages = {}
        self.running = False

        self.app(self)

    def addstage(self, status, stage):
        if status not in self.stages:
            self.stages[status] = []

        self.stages[status].append(stage)

    def start(self):
        self.running = True

        while self.running:
            if len(self.inputs) > 0:
                status = self.inputs[0]["status"] if "status" in self.inputs[0] else "entrypoint"
                input = self.inputs[0]["input"] if "input" in self.inputs[0] else None
                self.stages[status](input)

    def stop(self):
            self.running = False


def app(network):
    def main():
        for i in range(5):
            print(i)

        # Branch into a node to do a blocking task with status 'after-looper'.

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