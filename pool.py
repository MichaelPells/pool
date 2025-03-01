import sys
import threading

TIMEOUT = 60
NOMINAL_WORKERS = 5
MIN_WORKERS = 2
MAX_WORKERS = 1000

def ERROR_HANDLER(error, target, *args, **kwargs):
    sys.stderr.write(f'''An error occurred while handling a task.
Task: {target.__name__()}(*{args}, **{kwargs})
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

        self.prober = threading.Lock()
        self.waiter = threading.Lock()
        self.stopper = threading.Lock()

        self.working = False
        self.workers = {}
        self.idle = []
        self.new_worker_id = 0


    def idler(self, id):
        self.idle.append(id)

        # Unlock assign
        try:
            self.waiter.release()
        except RuntimeError:
            pass

        # Unlock stop
        try:
            self.stopper.release()
        except RuntimeError:
            pass
        log(f'Stopper released by {id}')

    def unidler(self, id):
        self.idle.remove(id)

    def start(self, number=0):
        if not number:
            number = self.nominal_workers

        self.working = True

        for n in range(number):
            self.new_worker_id += 1
            worker = Worker(self, self.new_worker_id)
            worker.start()

            self.workers[self.new_worker_id] = worker
            self.idle.append(self.new_worker_id)

        return self.new_worker_id
    
    def stop(self):
        self.working = False

        while len(self.workers):
            log(f'Stop is waiting for {self.workers}')
            # In the next iteration, Wait for `idle` to be updated.
            self.stopper.acquire()
            log(f'Stopper acquired for {self.idle}')

            while len(self.idle): # Should any worker have returned to `idle` before the last interation was over
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
    
    # This function assumes it is being run synchronously, and that no more than one instance of it runs at a time.
    def assign(self, target, args=(), kwargs={}, error_handler=None):
        if self.working:
            if not isinstance(target, Task):
                task = Task(target, args, kwargs, error_handler or self.error_handler) # Create a Task object
            else:
                task = target
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
            
            return task
        else:
            raise RuntimeError("Task assigned to a stopped pool.")


class Worker(threading.Thread):
    def __init__(self, pool=Pool(), id=0):
        threading.Thread.__init__(self)

        self.pool = pool
        self.id = id

        self.new = True
        self.task = None
        self.lock = threading.Lock()
        self.timed_out = False

    def run(self):
        while self.pool.working or self.task: # If pool is still working, or a rare case where task has been assigned between `idler()` call and `stop()` call.
            # Wait for task to be assigned, and worker unlocked.
            locked = self.lock.acquire(timeout=TIMEOUT)
            log(f'{self.id} lock: {locked}')

            # Handle timeout.
            if locked == False:
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


class Task:
    def __init__(self, target, args=(), kwargs={}, error_handler=None):
        self.action = target

        self.parameters = {
            "args": args,
            "kwargs": kwargs,
            "error_handler": error_handler
        }
        self.result = None

        self.started = False
        self.completed = False
        self.lock = threading.Lock()
        self.lock.acquire()
    
    def __call__(self):
        self.started = True
        self.result = self.action(*self.parameters["args"], **self.parameters["kwargs"])
        self.completed = True
        self.lock.release()


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

    pool.assign(sleeper, [25])
    
    pool.assign(sleeper, [20])
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    
    pool.assign(printer)
    

    time.sleep(10)
    pool.stop()

    time.sleep(25)
    print(pool.workers)
