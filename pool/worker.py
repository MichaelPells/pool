from pool.defaults import *
from pool.task import Task

import threading

def log(msg):
    if __name__ == "__main__":
        print(msg)

class Worker(threading.Thread):
    def __init__(self, pool, id=0):
        threading.Thread.__init__(self)

        self.pool = pool
        self.id = id

        self.active = True
        self.new = True
        self.task: Task = None
        self.lock = threading.Event()
        self.access = threading.Lock()
        self.timed_out = False

    def run(self):
        while self.active and (self.pool.working or self.pool.priorities or self.task): # If pool is still working/busy, or a rare case where task has been assigned between `idler()` call and `stop()` call.
            # Wait for task to be assigned, and worker unlocked.
            locked = self.lock.wait(timeout=TIMEOUT)
            self.lock.clear()
            log(f'{self.id} lock: {locked}')

            # Handle timeout.
            if not locked:
                # Probe the worker
                self.timed_out = True # First suspend the worker
                if not self.task: # Be sure a task has not been assigned between timeout expire and worker suspension.

                    # Killing a thread and recreating a new one is expensive.
                    # Simply renew an expired, unused thread or a scarce thread.
                    with self.pool.prober:
                        if not self.new and (self.pool.size > MIN_WORKERS or not self.id): # Kill the worker
                            log(f'{self.id} probing')
                            log(self.pool.workers)
                            log(self.pool.idle)
                            print(f"{self.id} timing out")
                            break
                        else: # Restore the worker
                            self.timed_out = False

                else: # Restore the worker
                    self.timed_out = False
                log(f'{self.id} ---------------- {self.timed_out}')

            # Execute task
            if self.task:
                if self.id: self.pool.unidler(self.id) # Unregister idleness
                self.new = False

                try:
                    self.task()
                except Exception as error:
                    # Handle exceptions with priority list -
                    # task's `error_handler` argument > Pool()'s `error_handler` argument > default `ERROR_HANDLER`
                    self.task.parameters["error_handler"](error, self.task, *self.task.parameters["args"], **self.task.parameters["kwargs"])

                self.task = None # Clear task.
                if self.id: self.pool.idler(self.id) # Register idleness.

        if self.id: self.resign()

    def assign(self, task):
        self.task = task

        # Unlock worker
        self.lock.set()

    def resign(self):
        self.new = False

        self.pool.size -= 1

        self.pool.unidler(self.id)
        del self.pool.workers[self.id]

        self.pool.delister.set()

        log(f'{self.id} killed')