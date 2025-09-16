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

from database import *

db =  Database()
columns = [("id", Number), "email", "firstname", "middlename", "surname", "gender", "country", "phone", "isstudent", "school", "referrer"]
entries = [[int(y.strip()) if y.isdigit() else (y.strip() if y.strip() else Null())
            for y in x.split(",")]
            for x in open("SampleData1.csv").read().splitlines()]
db.create("Table1", columns=columns, entries=entries, primarykey="id")

# # db.update("Table1", {"surname": "Akinpelumi"}, record={"country": "Canada"})
# db.update("Table1", NOT({"country": Var.any(["Nigeria", "NIGERIA", "Nigerian", "nigeria"])}), {"country": Var.NULL})
# result = db.read("Table1", {"country": Var.any([Var.NULL, "nigeria"])})

# for r in result.get():
#     print(r)
# print(result.count)

# db.update("Table1", {"id": 200}, record={"phone": Formula(lambda x, y: x - y, x=2, y=4)})
# result = db.read("Table1", {"id": 200})
# print(result.get(row=0, column="phone").retrieve())

# def controller(f, g):
#     if len(g.results) < 2:
#         try:
#             g.prev = f(g.prev)
#         except AttributeError:
#             g.prev = g.init
#     else:
#         g.prev = sum(g.results[-2:])
    
#     g.results.append(g.prev)

#     return g.prev

# y = Generator(0, lambda x: x + 1, controller, results = [])
# for n in range(10):
#     print(y.decode({}))

# y = Generator(Field(199, "id"), lambda Prev: Field(Prev.row + 1, "id"))

# db.update("Table1", OR({"id": 200}, {"id": 201}, {"id": 202}), record={"gender": This(lambda id: Field(id - 1, "id"), "id")})

# result = db.read("Table1", OR({"id": 200}, {"id": 201}, {"id": 202}))
# print([[row[0], row[1].retrieve()] for row in result.get(column=["id", "gender"])])


# db.update("Table1", {"id": 200}, record={"surname": "phone"})

# db.update("Table1", {"id": 300}, record={"isstudent": Field(1000, "gender")})
# # print(db.tables["Table1"]["references"])
# print(db.tables["Table1"]["indexes"]["isstudent"].keys())
# result = db.read("Table1", {"isstudent": Error()})
# print(result.count)
# print(result.get(row=0, column="isstudent").compute().args)

# db.update("Table1", {"id": 400}, record={"id": 1000, "gender": "Hello"})
# # print(db.tables["Table1"]["references"])
# print(db.tables["Table1"]["indexes"]["isstudent"].keys())

# result = db.read("Table1", {"id": 300})
# print(result.get(row=0, column="isstudent").compute())

# db.update("Table1", {"id": 1000}, record={"gender": "Hi"})
# print(db.tables["Table1"]["indexes"]["isstudent"].keys())


# db.update("Table1", {"id": 300}, record={"isstudent": Field(200, Field(1000, "surname"))})
# print(db.tables["Table1"]["references"])
# print(db.tables["Table1"]["indexes"]["isstudent"].keys())

# db.update("Table1", {"id": 400}, record={"id": 1000, "surname": "gender"})
# print(db.tables["Table1"]["references"])
# print(db.tables["Table1"]["indexes"]["isstudent"].keys())

# result = db.read("Table1", {"id": 300})
# print(result.get(row=0, column="isstudent").compute())

# db.update("Table1", {"id": 1000}, record={"gender": "Hi"})
# print(db.tables["Table1"]["indexes"]["isstudent"].keys())
# result = db.read("Table1", {"id": 300})
# print(result.get(row=0, column="isstudent").compute())


# db.update("Table1", {"id": 200}, record={"surname": "gender"})
# result = db.read("Table1", {"id": 300})
# print(result.get(row=0, column="phone").retrieve())
# print(db.tables["Table1"]["indexes"]["phone"][8140147440])
# print(db.tables["Table1"]["indexes"]["phone"]["Female"])

db.update("Table1", {"id": 300}, record={"phone": 1})
db.update("Table1", {"id": 200}, record={"id": Numbers.max("id")})
db.update("Table1", {"id": 300}, record={"phone": "1"})
result = db.read("Table1", {"id": 1})
print(result.count)
print(result.get(row=0, column="id"))
print(result.count)
