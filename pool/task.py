import sys
import threading
import io
import os

def ERROR_HANDLER(error, target, *args, **kwargs):
    sys.stderr.write(f'''An error occurred while handling a task.
Task: {target.id or "{0}(*{1}, **{2})".format(target.action.__name__, args, kwargs)}
Exception: {error}

''')

class TaskIO(io.TextIOWrapper):
    def __init__(self,
                encoding: str | None = None,
                errors: str | None = None,
                newline: str | None = None,
                line_buffering: bool = False,
                write_through: bool = False
                ):
        r, w = os.pipe()
        kwargs = {key:value for (key, value) in {"encoding": encoding, "errors": errors, "newline": newline}.items() if value}
        self.r, self.w = os.fdopen(r, "r", **kwargs), os.fdopen(w, "w", **kwargs)
        
        super().__init__(self.r.buffer, encoding, errors, newline, line_buffering, write_through)
        self.mode = "r+"

        self.flush = self.w.flush
        self.truncate = self.w.truncate
        self.writable = self.w.writable
        self.write = self.w.write
        self.writelines = self.w.writelines

    def close(self):
        self.r.close()
        self.w.close()

    # The following do not work:
        # seek()
        # tell()
        # truncate()

class Task:
    def __init__(self, target, args=(), kwargs={}, error_handler=ERROR_HANDLER, id=None, priority=1, weight=1, interactive=False):
        self.action = target
        self.id = id
        self.priority = priority
        self.weight = weight
        self.interactive = interactive
        if self.interactive: self.interact()

        self.parameters = {
            "args": args,
            "kwargs": kwargs,
            "error_handler": error_handler
        }

        self.listeners = {
            "once": {},
            "on": {}
        }

        self.attempts = 0
        self.completes = 0
        self.fails = 0

        self.result = None

        self.started = False
        self.completed = False
        self.status = "pending"

        self.lock = threading.Event()

    def reset(self):
        self.result = None
        self.started = False
        self.completed = False
        self.status = "pending"

        self.lock.clear()

    def __call__(self):
        self.attempts += 1
        self.setstatus("started")

        try:
            if self.interactive:
                self.result = self.action(self, *self.parameters["args"], **self.parameters["kwargs"])
            else:
                self.result = self.action(*self.parameters["args"], **self.parameters["kwargs"])
        except Exception as error:
            self.fails += 1
            self.lock.set()
            self.setstatus("failed")
            raise error
        else:
            self.completes += 1
            self.lock.set()
            self.setstatus("completed")

    def interact(self):
        self.interactive = True
        self.stdin, self.stdout, self.stderr = TaskIO(), TaskIO(), TaskIO() # Should all 3 be created even if not needed?

    def setstatus(self, status):
        self.status = status

        if status == "started":
            self.started = True
            self.emit("started")
        elif status == "completed":
            self.completed = True
            self.emit("completed")
        elif status == "failed":
            self.completed = True
            self.emit("failed")

        self.emit("statuschange", status)

    def once(self, event, action):
        if event not in self.listeners["once"]:
            self.listeners["once"][event] = []
        self.listeners["once"][event].append(action)

    def on(self, event: str, action):
        if event not in self.listeners["on"]:
            self.listeners["on"][event] = []
        self.listeners["on"][event].append(action)

    def emit(self, event, *args): # Should this be merged with `setstatus` later, for space management?
        if event in self.listeners["once"]:
            for action in self.listeners["once"][event]:
                action(*args)
            del self.listeners["once"][event]

        if event in self.listeners["on"]:
            for action in self.listeners["on"][event]:
                action(*args)

    def wait(self, timeout=None):
        if timeout == None:
            return self.lock.wait()
        else:
            return self.lock.wait(timeout=timeout)

    def getresult(self):
        self.lock.wait()
        return self.result
