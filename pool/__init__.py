from pool.defaults import *
from pool.task import *
from pool.worker import *

# Use kwargs in function calls
import threading

class Access(threading.Event):
    def __init__(self, resource):
        super().__init__()
        self.resource = resource
    
    def close(self):
        self.resource.restricted = True

    def open(self):
        self.resource.restricted = False
        self.set()
        self.clear()

    def request(self):
        if self.resource.restricted:
            self.wait()
  

class Input:
    def __init__(self, args: tuple | dict = (), kwargs: dict | None = None):
        if type(args) == tuple:
            self.args = args

            if type(kwargs) == dict or kwargs is None:
                self.kwargs = kwargs or {}
            else:
                raise TypeError("kwargs can only be a dictionary or None.")
            
        elif type(args) == dict:
            self.args = ()
            self.kwargs = args

            if kwargs != None:
                raise TypeError("kwargs can only be None when args is a dictionary.")
            
        else:
            raise TypeError("args can only be a tuple or dictionary.")


class Pool:
    def __init__(self,
                 timeout=TIMEOUT,
                 nominal_workers=NOMINAL_WORKERS,
                 min_workers=MIN_WORKERS,
                 max_workers=MAX_WORKERS,
                 max_backlog=0,
                 min_backlog=0,
                 error_handler=ERROR_HANDLER,
                 priority_levels=PRIORITY_LEVELS
                ):

        self.timeout = timeout
        self.nominal_workers = nominal_workers
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.max_backlog = max_backlog
        self.min_backlog = min_backlog or max_backlog
        self.error_handler = error_handler
        self.priority_levels = priority_levels

        self.queuer = threading.Lock()
        self.hiring = threading.Lock()
        self.prober = threading.Lock()
        self.waiter = threading.Event()
        self.stopper = threading.Event()
        self.delister = threading.Event()

        self.working = False
        self.workers = {}
        self.idle = []
        self.size = 0
        self.members = {}
        self.backlog = 0

        self.queue = {p: [] for p in range(self.priority_levels)}
        self.priorities = {}
        self.prioritiesupdated = True
        self.restricted = False
        self.access = Access(self)
        self.pending = {p: 0 for p in range(self.priority_levels)}
        self.focus: None | int = None

        self._manager = Worker(self)
        self._hirer = Worker(self)

    def idler(self, id):
        if self.workers[id].active:
            self.idle.append(id)

            # Unlock manager
            self.waiter.set()

    def unidler(self, id):
        if id in self.idle:
            self.idle.remove(id)

            # Unlock hirer
            try:
                self.hiring.release()
            except RuntimeError:
                pass

    def hire(self, workers=1):
        for _ in range(workers):
            id = self.size + 1
            worker = Worker(self, id)
            worker.start()

            self.workers[id] = worker
            self.size = id
            self.idler(id)

        return self.size

    def fire(self, id=None):
        if id == None:
            id = self.size

        if id in self.workers:
            worker = self.workers[id]
            if worker.new and self.working:
                return

            with worker.access:
                worker.active = False
                worker.lock.set() # Unlock worker

        else:
            raise RuntimeError(f"Attempted to fire an unknown worker ({id}).")

    def start(self, workers=0): # Pool is currently not being reset after stop(). Re-start-ing might crash (e.g because of initially acquired locks).
        if not self.working:
            if not workers:
                workers = self.nominal_workers

            self.working = True

            self._manager.start()
            self._manager.assign(Task(self.manager, id="manager"))

            self._hirer.start()
            self._hirer.assign(Task(self.hirer, id="hirer"))

            self.hire(workers)

        else:
            raise RuntimeError("Pool started already.")

    def stop(self):
        if self.working:
            self.working = False

            try:
                self.hiring.release()
            except RuntimeError:
                pass

            size = self.size

            self._hirer.active = False
            self._hirer.lock.set() # Unlock hirer

            try:
                self.queuer.release()
            except RuntimeError:
                pass

            self._manager.active = False
            self._manager.lock.set() # Unlock manager

            self.stopper.wait()
            self.stopper.clear()

            for role in dict(self.members):
                self.terminate(role)

            for id in self.idle:
                try:
                    self.fire(id)
                except (RuntimeError, KeyError):
                    pass

            self.delister.set()
            while self.size:
                self.delister.wait() # Wait for next worker(s) to be delisted.
                self.delister.clear()

            print(size)

        else:
            raise RuntimeError("Pool stopped already.")

    def member(self, member, routine, interactive):
        while not member.completed or member.priorities:
            member.operate.acquire()

            while True:
                try:
                    if member.prioritiesupdated:
                        focus = member.focus = max(member.priorities) # `ValueError` Exception when `member.priorities` is empty.
                        member.prioritiesupdated = False

                    operation = member.operations[focus].pop(0)

                except ValueError:
                    member.focus = None
                    break

                try:
                    operation()
                except Exception:
                    ...
                finally:
                    if "supervisor" in member.__dict__:
                        with member.idler:
                            idleness = member.idleness
                            member.supervisor.team["idleness"][idleness].remove(member)
                            if not member.supervisor.team["idleness"][idleness]:
                                del member.supervisor.team["idleness"][idleness]

                            idleness -= operation.weight
                            if idleness not in member.supervisor.team["idleness"]:
                                member.supervisor.team["idleness"][idleness] = []
                            member.supervisor.team["idleness"][idleness].append(member)
                            member.idleness = idleness

                if not member.operations[focus]:
                    member.access.close()

                    while member.pending[focus]: pass
                    if not member.operations[focus]:
                        del member.priorities[focus]
                        member.prioritiesupdated = True

                    member.access.open()

        # May be return a log of performance later.

    def appoint(self, routine, role, error_handler=None, interactive=False):
        if self.working:
            member = Task(self.member, args=(routine, interactive), error_handler=error_handler, id=role, interactive=True)

            member.__setattr__("operate", threading.Lock())
            member.__setattr__("operations", {p: [] for p in range(self.priority_levels)})
            member.__setattr__("priorities", {})
            member.__setattr__("prioritiesupdated", True)
            member.__setattr__("restricted", False)
            member.__setattr__("access", Access(member))
            member.__setattr__("pending", {p: 0 for p in range(self.priority_levels)})
            member.__setattr__("focus", None)

            self.execute(member)
            self.members[role] = member

            return role
        else:
            raise RuntimeError("Role assigned within a stopped pool.")

    def supervisor(self, supervisor, routine, interactive):
        while not supervisor.completed or supervisor.priorities:
            supervisor.operate.acquire()

            while True:
                try:
                    if supervisor.prioritiesupdated:
                        focus = supervisor.focus = max(supervisor.priorities) # `ValueError` Exception when `supervisor.priorities` is empty.
                        supervisor.prioritiesupdated = False

                    operation = supervisor.operations[focus].pop(0)

                except ValueError:
                    supervisor.focus = None
                    break

                idlest = min(supervisor.team["idleness"].keys())
                member = supervisor.team["idleness"][idlest][0]
                
                with member.idler:
                    idleness = member.idleness
                    supervisor.team["idleness"][idleness].remove(member)
                    if not supervisor.team["idleness"][idleness]:
                        del supervisor.team["idleness"][idleness]

                    idleness += operation.weight
                    if idleness not in supervisor.team["idleness"]:
                        supervisor.team["idleness"][idleness] = []
                    supervisor.team["idleness"][idleness].append(member)
                    member.idleness = idleness

                if operation.priority == member.focus:
                    member.access.request()

                member.pending[operation.priority] += 1 # Enlisting

                if operation.priority == member.focus and member.restricted:
                    unlisted = True
                    member.pending[operation.priority] -= 1
                    member.access.wait()
                else:
                    unlisted = False

                member.operations[operation.priority].append(operation)
                if operation.priority not in member.priorities:
                    member.priorities[operation.priority] = None
                    member.prioritiesupdated = True

                if not unlisted:
                    member.pending[operation.priority] -= 1

                try:
                    member.operate.release()
                except RuntimeError:
                    pass

                if not supervisor.operations[focus]:
                    supervisor.access.close()

                    while supervisor.pending[focus]: pass
                    if not supervisor.operations[focus]:
                        del supervisor.priorities[focus]
                        supervisor.prioritiesupdated = True

                    supervisor.access.open()

        # May be return a log of performance later.

    def team(self, workers, routine, role, error_handler=None, interactive=False):
        if self.working:
            supervisor = Task(self.supervisor, args=(routine, interactive), error_handler=error_handler, id=role, interactive=True)

            supervisor.__setattr__("team", {
                "members": [],
                "idleness": {
                    0: []
                    }
            })
            supervisor.__setattr__("operate", threading.Lock())
            supervisor.__setattr__("operations", {p: [] for p in range(self.priority_levels)})
            supervisor.__setattr__("priorities", {})
            supervisor.__setattr__("prioritiesupdated", True)
            supervisor.__setattr__("restricted", False)
            supervisor.__setattr__("access", Access(supervisor))
            supervisor.__setattr__("pending", {p: 0 for p in range(self.priority_levels)})
            supervisor.__setattr__("focus", None)

            def dissolver():
                for member in supervisor.team["members"]:
                    member.completed = True
                    try:
                        member.operate.release()
                    except RuntimeError:
                        pass
            supervisor.once("completed", dissolver)

            self.execute(supervisor)
            self.members[role] = supervisor

            for n in range(workers):
                member = Task(self.member, args=(routine, interactive), error_handler=error_handler, id=f"{role}-{n}", interactive=True)

                member.__setattr__("supervisor", supervisor)
                member.__setattr__("idleness", 0)
                member.__setattr__("idler", threading.Lock())
                member.__setattr__("operate", threading.Lock())
                member.__setattr__("operations", {p: [] for p in range(self.priority_levels)})
                member.__setattr__("priorities", {})
                member.__setattr__("prioritiesupdated", True)
                member.__setattr__("restricted", False)
                member.__setattr__("access", Access(member))
                member.__setattr__("pending", {p: 0 for p in range(self.priority_levels)})
                member.__setattr__("focus", None)

                self.execute(member)

                supervisor.team["members"].append(member)
                supervisor.team["idleness"][0].append(member)

            return role
        else:
            raise RuntimeError("Role assigned within a stopped pool.")

    def dissolve(self, team):
        if team in self.members:
            self.terminate(team)
        else:
            raise RuntimeError(f"Attempted to dissolve an unknown team ({team}).")

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

    def assign(self, role, Input, priority=1, behaviour=None):
        if self.working:
            if role in self.members:
                member = self.members[role]
                member_params = member.parameters["args"]
                operation = Task(member_params[0], Input.args, Input.kwargs, priority=priority, interactive=member_params[1])

                if behaviour:
                    behaviour(operation)

                if operation.priority == member.focus:
                    member.access.request()

                member.pending[operation.priority] += 1 # Enlisting

                if operation.priority == member.focus and member.restricted:
                    unlisted = True
                    member.pending[operation.priority] -= 1
                    member.access.wait()
                else:
                    unlisted = False

                member.operations[operation.priority].append(operation)
                if operation.priority not in member.priorities:
                    member.priorities[operation.priority] = None
                    member.prioritiesupdated = True

                if not unlisted:
                    member.pending[operation.priority] -= 1

                try:
                    member.operate.release()
                except RuntimeError:
                    pass

                return operation
            else:
                raise RuntimeError(f"Operation assigned to an unidentified role ({role}).")
        else:
            raise RuntimeError("Operation assigned within a stopped pool.")

    def execute(self, target, args=(), kwargs={}, error_handler=None, priority=1, interactive=False, behaviour=None):
        if self.working:
            if not isinstance(target, Task):
                task = Task(target, args, kwargs, error_handler or self.error_handler, priority=priority, interactive=interactive) # Create a Task object
            else:
                task = target

            if behaviour:
                behaviour(task)

            if task.priority == self.focus:
                self.access.request()

            self.pending[task.priority] += 1 # Enlisting

            if task.priority == self.focus and self.restricted:
                unlisted = True
                self.pending[task.priority] -= 1
                self.access.wait()
            else:
                unlisted = False

            self.queue[task.priority].append(task)
            if task.priority not in self.priorities:
                self.priorities[task.priority] = None
                self.prioritiesupdated = True

            self.backlog += task.weight

            if not unlisted:
                self.pending[task.priority] -= 1

            try:
                self.queuer.release()
            except RuntimeError:
                pass
            
            return task
        else:
            raise RuntimeError("Task assigned to a stopped pool.")

    def manager(self):
        while self.working or self.priorities:
            self.queuer.acquire()

            while True:
                try:
                    if self.prioritiesupdated:
                        focus = self.focus = max(self.priorities) # `ValueError` Exception when `self.priorities` is empty.
                        self.prioritiesupdated = False

                    task = self.queue[focus].pop(0)

                except ValueError:
                    self.focus = None
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
                        with worker.access:
                            if worker.active and not worker.timed_out and not worker.task: # Avoiding race conditions, majorly. Workers don't get popped from `idle` instantly.
                                worker.assign(task) # Assign the task to this worker

                                break
                        n += 1
                except IndexError: # Means we reached the end of idle, yet no available worker
                    while True:
                        self.waiter.wait() # Wait for `idle` to be updated.
                        self.waiter.clear()

                        worker = self.workers[self.idle[0]]
                        with worker.access:
                            if worker.active:
                                worker.assign(task) # Assign the task to this worker

                                break

                self.backlog -= task.weight

                if not self.queue[focus]:
                    self.access.close()

                    while self.pending[focus]: pass
                    if not self.queue[focus]:
                        del self.priorities[focus]
                        self.prioritiesupdated = True

                    self.access.open()
        else:
            try:
                self.queuer.release()
            except RuntimeError:
                pass

            self.stopper.set()

    def hirer(self):
        full = False

        while self.working:
            self.hiring.acquire()

            if self.working:
                if not self.idle:
                    if not full:
                        if self.backlog >= self.max_backlog:
                            full = True

                    if full:
                        if self.backlog > self.max_backlog:
                            with self.prober:
                                if self.size < MAX_WORKERS:
                                    print("hired")
                                    self.hire()

                        elif self.backlog < self.min_backlog:
                            with self.prober:
                                if self.size > MIN_WORKERS:
                                    print("fired")
                                    while len(self.idle) < min(3, self.size): pass
                                    self.fire()
        else:
            try:
                self.hiring.release()
            except RuntimeError:
                pass
