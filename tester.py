# # import threading, time

# # event = threading.Event()

# # def tester():
# #     print("started")
# #     event.wait()
# #     print("ended")

# # threading.Thread(target=tester).start()

# # time.sleep(4)
# # event.set()
# # event.clear()

# # threading.Thread(target=tester).start()

# # time.sleep(4)
# # event.set()

# from pool import *

# # pool = Pool()
# # pool.start()

# # def printer(x):
# #     print(x)

# # pool.team(3, printer, "printer")
# # pool.assign("printer", Input(("hello",)))
# # pool.stop()

# from pool import Pool, Task
# import time

# pool = Pool(priority_levels=7, max_backlog=9000, min_backlog=8000)
# pool.start(10)

# def printer(p, n):
#     print(f"{p} - {n}")

# for n in range(10000):
#     priority = n % 7
#     pool.execute(printer, priority=priority, args=(priority, n))
# time.sleep(10)
# pool.stop()

# # from pool import Pool, Task
# # import threading
# # import time

# # pool = Pool()
# # pool.start()

# # def sleeper(task: Task):
# #     print(task.attempts)

# # task = Task(sleeper, interactive=True)

# # def restarter():
# #     if task.attempts < 1000:
# #         task.reset()
# #         pool.execute(task)
# #     else:
# #         threading.Thread(target=pool.stop).start()
# # task.on("completed", restarter)

# # pool.execute(task)

# # from pool import Pool, Task

# # pool = Pool()
# # pool.start()

# # def doubler(x):
# #     return x * 2
# # pool.appoint(doubler, "doubler")

# # operation = pool.assign("doubler", args=(2,))
# # operation.wait()
# # print(operation.status)

# # # pool.assign("doubler", args=(3,))
# # # pool.assign("doubler", args=(4,))
# # # pool.assign("doubler", args=(5,))
# # # pool.assign("doubler", args=(6,))
# # # pool.assign("doubler", args=(7,))
# # # pool.assign("doubler", args=(8,))
# # # pool.assign("doubler", args=(9,))
# # pool.stop()

# # from pool import Pool, Task
# # import time

# # import threading, multiprocessing

# # pool = Pool()
# # pool.start()

# # def sleeper():
# #     time.sleep(10)
# #     return "Done"

# # task = Task(sleeper)
# # threading.Thread(target=task).start()
# # # multiprocessing.Process(target=task).start()
# # print(task.getresult())

# # import os
# # import io

# # class TaskIO(io.TextIOWrapper):
# #     def __init__(self,
# #                 encoding: str | None = None,
# #                 errors: str | None = None,
# #                 newline: str | None = None,
# #                 line_buffering: bool = True,
# #                 write_through: bool = True
# #                 ):
# #         r, w = os.pipe()
# #         kwargs = {key:value for (key, value) in {"encoding": encoding, "errors": errors, "newline": newline}.items() if value}
# #         self.r, self.w = os.fdopen(r, "r", **kwargs), os.fdopen(w, "w", **kwargs)
        
# #         super().__init__(self.r.buffer, encoding, errors, newline, line_buffering, write_through)
# #         self.mode = "r+"

# #         self.flush = self.w.flush
# #         self.truncate = self.w.truncate
# #         self.writable = self.w.writable
# #         self.write = self.w.write
# #         self.writelines = self.w.writelines

# #     def close(self):
# #         self.r.close()
# #         self.w.close()

# #     # The following do not work:
# #         # seek()
# #         # tell()
# #         # truncate()

# # # file = TaskIO()
# # # file.write("Hello")
# # # file.write("Hi\n")
# # # file.flush()

# # # print(file.read(3))
# # # print(file.tell())
# # # print(os.lseek(file.fileno(), 0, 0))
# # # print(file.read(3))
# # # print(file.tell())


# # # r, w = os.pipe()
# # # x, y = os.fdopen(r, "r"), os.fdopen(w, "w")

# # # y.write("Hello")
# # # y.flush()

# # r, w = os.pipe()
# # x = open("hello", "w")
# # y = os.fdopen(w, "wb")
# # z = os.fdopen(r, "rb")
# # y.write(x.buffer)
# # x.write("Hello\nHi\n")
# # print(z.read(3))

# class Null:
#     def __len__(self):
#         return 0
# NULL = Null()
# print(NULL or "Yes")

from train import Database

database =  Database()
columns = ["id", "email", "firstname", "middlename", "surname", "gender", "country", "phone", "isstudent", "school"]
entries = [x[:-1].split(",") for x in open("SampleData1.csv").read().splitlines()]
database.create("Table1", columns=columns, entries=entries)

result = database.view("Table1", {"surname": "Akinpelumi"})

print(result)