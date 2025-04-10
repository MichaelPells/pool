# Use kwargs in function calls
# Use with statement in locks where possible
import sys
import threading
import io
import os

TIMEOUT = 60
NOMINAL_WORKERS = 5
MIN_WORKERS = 2
MAX_WORKERS = 1000

def ERROR_HANDLER(error, target, *args, **kwargs):
    sys.stderr.write(f'''An error occurred while handling a task.
Task: {target.id or "{0}(*{1}, **{2})".format(target.action.__name__, args, kwargs)}
Exception: {error}

''')

class Pool:
    def __init__(self,
                 timeout=TIMEOUT,
                 nominal_workers=NOMINAL_WORKERS,
                 min_workers=MIN_WORKERS,
                 max_workers=MAX_WORKERS,
                 error_handler=ERROR_HANDLER
                ):

        self.timeout = timeout
        self.nominal_workers = nominal_workers
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.error_handler = error_handler

        self.queuer = threading.Lock()
        self.prober = threading.Lock()
        self.waiter = threading.Lock()
        self.stopper1 = threading.Lock()
        self.stopper2 = threading.Lock()

        self.working = False
        self.workers = {}
        self.idle = []
        self.new_worker_id = 0
        self.members = {}
        self.queue = []

    def idler(self, id):
        self.idle.append(id)

        # Unlock assign
        try:
            self.waiter.release()
        except RuntimeError:
            pass

        # Unlock stop
        try:
            self.stopper2.release()
        except RuntimeError:
            pass
        log(f'Stopper released by {id}')

    def unidler(self, id):
        self.idle.remove(id)

    def start(self, workers=0): # Pool is currently not being reset after stop(). Re-start-ing might crash (e.g because of initially acquired locks).
        if not workers:
            workers = self.nominal_workers

        if not self.working:
            self.working = True
            resumed = True
        else:
            resumed = False

        for _ in range(workers):
            self.new_worker_id += 1
            worker = Worker(self, self.new_worker_id)
            worker.start()

            self.workers[self.new_worker_id] = worker
            self.idle.append(self.new_worker_id)

        if resumed:
            threading.Thread(target=self.manager).start()

        return self.new_worker_id

    def stop(self):
        self.working = False

        if self.queue:
            self.stopper1.acquire()
            self.stopper1.acquire()

        for role in dict(self.members):
            self.terminate(role)

        while self.workers:
            log(f'Stop is waiting for {self.workers}')
            # In the next iteration, Wait for `idle` to be updated.
            self.stopper2.acquire()
            log(f'Stopper acquired for {self.idle}')

            while self.idle: # Should any worker have returned to `idle` before the last interation was over
                for id in self.idle:
                    worker = self.workers[id]

                    if not worker.task: # Be sure `stop()` was not called between a task assignment and worker unlock.
                        try:
                            worker.lock.release() # Unlock worker
                        except RuntimeError:
                            pass

                        self.idle.remove(id)
                        del self.workers[id]
                        log(f'{id} killed')

        try:
            self.queuer.release()
        except RuntimeError:
            pass

    def member(self, member, routine, interactive):
        while not member.completed or member.operations:
            member.operate.acquire()

            while True:
                try:
                    operation = member.operations.pop(0)
                except IndexError:
                    break

                try:
                    operation()
                except Exception:
                    ...

        # May be return a log of performance later.

    def appoint(self, routine, role, error_handler=None, interactive=False):
        if self.working:
            member = Task(self.member, args=(routine, interactive), error_handler=error_handler, id=role, interactive=True)
            self.assign(member)
            self.members[role] = member

            return role
        else:
            raise RuntimeError("Role assigned within a stopped pool.")

    def terminate(self, role):
        if role in self.members:
            member = self.members[role]
            member.completed = True

            try:
                member.operate.release()
            except RuntimeError:
                pass
            
            del self.members[role]
        else:
            raise RuntimeError(f"Attempted to terminate an unidentified role ({role}).")

    def assign2(self, role, args=(), kwargs={}, behaviour=None):
        if self.working:
            if role in self.members:
                member = self.members[role]
                member_params = member.parameters["args"]
                operation = Task(member_params[0], args, kwargs, interactive=member_params[1])

                if behaviour:
                    behaviour(operation)

                member.operations.append(operation)

                try:
                    member.operate.release()
                except RuntimeError:
                    pass

                return operation
            else:
                raise RuntimeError(f"Operation assigned to an unidentified role ({role}).")
        else:
            raise RuntimeError("Operation assigned within a stopped pool.")

    def assign(self, target, args=(), kwargs={}, error_handler=None, interactive=False, behaviour=None):
        if self.working:
            if not isinstance(target, Task):
                task = Task(target, args, kwargs, error_handler or self.error_handler, interactive=interactive) # Create a Task object
            else:
                task = target

            if behaviour:
                behaviour(task)

            self.queue.append(task)

            try:
                self.queuer.release()
            except RuntimeError:
                pass
            
            return task
        else:
            raise RuntimeError("Task assigned to a stopped pool.")

    def manager(self):
        while self.working or self.queue:
            self.queuer.acquire()

            while True:
                try:
                    task = self.queue.pop(0)
                except IndexError:
                    break

                # Looking for an available worker
                # Note: This whole section (especially loop) could have been done differently.
                # But I'm not sure timeliness would be guaranteed
                try:
                    n = 0
                    while True:
                        # Worker is ideally `idle[0]`.
                        # But who can tell? Worker may not be ready for next task instantly, as exemplified below.
                        # Hence, the loop and checks.
                        worker = self.workers[self.idle[n]]
                        if not worker.timed_out and not worker.task: # Avoiding race conditions, majorly. Workers don't get popped from `idle` instantly.
                            worker.task = task # Assign the task to this worker
                            try:
                                worker.lock.release() # Unlock worker
                            except RuntimeError:
                                pass

                            # Taking a peep into `idle` for the next worker.
                            # Ideally the worker at `idle[1]`. But this is not always the case, as exemplified above.
                            # Create a new worker if none.
                            # This ensures there is always at least 1 available worker for the next incoming task.
                            try:
                                self.idle[n + 1]
                            except IndexError:
                                if len(self.workers) < MAX_WORKERS:
                                    self.start(1)

                            break
                        n += 1
                except IndexError: # Means we reached the end of idle, yet no available worker
                    if len(self.workers) < MAX_WORKERS:
                        new_worker_id = self.start(1) # Create a new worker
                        worker = self.workers[new_worker_id]
                        worker.task = task # Assign the task to this worker
                        try:
                            worker.lock.release() # Unlock worker
                        except RuntimeError:
                            pass
                    else:
                        # Wait for `idle` to be updated.
                        self.waiter.acquire()
                        self.waiter.acquire()

                        self.assign(task)
        else:
            try:
                self.stopper1.release()
            except RuntimeError:
                pass


class Worker(threading.Thread):
    def __init__(self, pool=Pool(), id=0):
        threading.Thread.__init__(self)

        self.pool = pool
        self.id = id

        self.new = True
        self.task: Task = None
        self.lock = threading.Lock()
        self.timed_out = False

    def run(self):
        while self.pool.working or self.task: # If pool is still working, or a rare case where task has been assigned between `idler()` call and `stop()` call.
            # Wait for task to be assigned, and worker unlocked.
            locked = self.lock.acquire(timeout=TIMEOUT)
            log(f'{self.id} lock: {locked}')

            # Handle timeout.
            if not locked:
                # Probe the worker
                self.timed_out = True # First suspend the worker
                if not self.task: # Be sure a task has not been assigned between timeout expire and worker suspension.

                    # Killing a thread and recreating a new one is expensive.
                    # Simply renew an expired, unused thread or a scarce thread.
                    with self.pool.prober:
                        if not self.new and len(self.pool.workers) > MIN_WORKERS: # Kill the worker
                            log(f'{self.id} probing')
                            self.pool.idle.remove(self.id)
                            del self.pool.workers[self.id]
                            log(f'{self.id} killed')
                            log(self.pool.workers)
                            log(self.pool.idle)
                            break
                        else: # Restore the worker
                            self.timed_out = False

                else: # Restore the worker
                    self.timed_out = False
                log(f'{self.id} ---------------- {self.timed_out}')

            # Execute task
            if self.task:
                self.pool.unidler(self.id) # Unregister idleness
                self.new = False

                try:
                    self.task()
                except Exception as error:
                    # Handle exceptions with priority list -
                    # task's `error_handler` argument > Pool()'s `error_handler` argument > default `ERROR_HANDLER`
                    self.task.parameters["error_handler"](error, self.task, *self.task.parameters["args"], **self.task.parameters["kwargs"])

                self.task = None # Clear task.
                self.pool.idler(self.id) # Register idleness.

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
    def __init__(self, target, args=(), kwargs={}, error_handler=ERROR_HANDLER, id=None, interactive=False): # Should `listeners` also be here?
        self.action = target
        self.id = id
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

        self.operations = []
        self.operate = threading.Lock()

        self.lock = threading.Lock()
        self.lock.acquire()

    def reset(self):
        self.result = None
        self.started = False
        self.completed = False
        self.status = "pending"
        self.operations = []

        self.lock.acquire()

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
            self.setstatus("failed")
            self.lock.release()
            raise error
        else:
            self.completes += 1
            self.setstatus("completed")
            self.lock.release()

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
            return self.lock.acquire()
        else:
            return self.lock.acquire(timeout=timeout)

    def getresult(self):
        with self.lock:
            return self.result

def log(msg):
    if __name__ == "__main__":
        print(msg)


if __name__ == "__main__":
    import time

    pool = Pool()

    n = 0
    def sleeper(t):
        global n
        n += 1
        print(f'Sleeper -------------------------------------------- {n}')
        time.sleep(t)

    def printer():
        global n
        n += 1
        print(f'---------------------------------------------------- {n}')

    pool.start()
    # workers[1].new = False
    # workers[2].new = False
    # workers[3].new = False
    # workers[4].new = False
    # workers[5].new = False
    print(pool.workers)
    print(pool.idle)

    # pool.assign(sleeper, [25])
    
    # pool.assign(sleeper, [20])
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    
    # pool.assign(printer)
    

    # time.sleep(10)
    # pool.stop()

    # time.sleep(25)
    # print(pool.workers)

    def test(task : Task):
        print(task.status)
        task.setstatus("running")
        x = task.stdin.readline().strip()
        print(task.stdin.tell())
        y = x * 2
        print(y)
        task.stdout.write(y + "\n")
        task.stdout.flush()
        task.setstatus("finishing")
        print(task.status)

    def statuser(status="Yessssssss"):
        print("Status Changed:", status)

    task = Task(test, interactive=True)
    task.on("statuschange", statuser)
    task.on("started", statuser)
    task.on("completed", statuser)

    pool.assign(task)
    task.stdin.write("Hello\n")
    task.stdin.flush()
    print(task.stdout.readline())
    # print(task.stdout.read(4))
    print(task.stdout.tell())
