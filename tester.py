from pool import Pool, Task

# pool = Pool()
# pool.start()

def sleeper():
    print("Done")

task = Task(sleeper)

def restarter():
    task.reset()
    task()
task.on("completed", restarter)

task()

# from pool import Pool, Task
# import time

# import threading, multiprocessing

# pool = Pool()
# pool.start()

# def sleeper():
#     time.sleep(10)
#     return "Done"

# task = Task(sleeper)
# threading.Thread(target=task).start()
# # multiprocessing.Process(target=task).start()
# print(task.getresult())

# import os
# import io

# class TaskIO(io.TextIOWrapper):
#     def __init__(self,
#                 encoding: str | None = None,
#                 errors: str | None = None,
#                 newline: str | None = None,
#                 line_buffering: bool = True,
#                 write_through: bool = True
#                 ):
#         r, w = os.pipe()
#         kwargs = {key:value for (key, value) in {"encoding": encoding, "errors": errors, "newline": newline}.items() if value}
#         self.r, self.w = os.fdopen(r, "r", **kwargs), os.fdopen(w, "w", **kwargs)
        
#         super().__init__(self.r.buffer, encoding, errors, newline, line_buffering, write_through)
#         self.mode = "r+"

#         self.flush = self.w.flush
#         self.truncate = self.w.truncate
#         self.writable = self.w.writable
#         self.write = self.w.write
#         self.writelines = self.w.writelines

#     def close(self):
#         self.r.close()
#         self.w.close()

#     # The following do not work:
#         # seek()
#         # tell()
#         # truncate()

# file = TaskIO()
# file.write("Hello")
# file.write("Hi\n")
# file.flush()

# print(file.read(3))
# print(file.tell())
# print(file.read(3))
# print(file.tell())
