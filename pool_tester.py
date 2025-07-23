from pool import *

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

    # pool.execute(sleeper, [25])
    
    # pool.execute(sleeper, [20])
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    
    # pool.execute(printer)
    

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

    pool.execute(task)
    task.stdin.write("Hello\n")
    task.stdin.flush()
    print(task.stdout.readline())
    # print(task.stdout.read(4))
    print(task.stdout.tell())
