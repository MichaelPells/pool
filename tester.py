from pool import Pool, Task
import time

import threading, multiprocessing

pool = Pool()
pool.start()

def sleeper():
    time.sleep(10)
    return "Done"

task = Task(sleeper)
multiprocessing.Process(target=task).start()
print(task.getresult())
