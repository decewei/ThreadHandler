from multiprocessing import Process, Pool
from collections import deque
from threading import Thread

# Using thread because I don't want to manage shared variable
# If using MP, the shared variable in class (e.g. self.VAR) will be copied to the new memory space, any modification
# to those self.VAR will be lost


class ThreadHandler:
    def __init__(self, fn, args, max_threads, bar=None):
        self.max = max_threads
        self.runQ = deque([])
        self.args = deque(args)
        self.fn = fn
        self.numfuncs = len(self.args)
        self.can_end = False
        self.bar = bar

    def terminate_dead(self):
        # examine every thread, terminate if it is not alive,
        # and clear space in runQ for new thread
        while not self.can_end:
            try:
                th = self.runQ.pop()
                if not th.is_alive():
                    th.join()
                    if self.bar is not None:
                        self.bar.update()
                else:
                    self.runQ.appendleft(th)
            except IndexError:
                pass

    def join(self):
        for th in self.runQ:
            th.join()
            if self.bar is not None:
                self.bar.update()

    def load_threads(self):
        # keep adding running thread until max thread reached
        while not self.can_end:
            while len(self.runQ) < self.max:
                try:
                    arg = self.args.pop()
                    p = Thread(target=self.fn, args=(arg,))
                    p.start()
                    self.runQ.appendleft(p)
                except IndexError:
                    break

    def start(self):
        thread1 = Thread(target=self.load_threads)
        thread2 = Thread(target=self.terminate_dead)
        thread1.start()
        thread2.start()
        while len(self.args) != 0:
            pass

        self.can_end = True

        thread1.join()
        thread2.join()
        self.join()


class ProcessHandler:
    def __init__(self, fn, args, max_threads, bar=None):
        self.max = max_threads
        self.runQ = deque([])
        self.args = deque(args)
        self.args_pure = args
        self.fn = fn
        self.numfuncs = len(self.args)
        self.bar = bar

    def terminate_dead(self):
        # examine every thread, terminate if it is not alive,
        # and clear space in runQ for new thread
        for _ in range(len(self.runQ)):
            try:
                th = self.runQ.pop()
                if not th.is_alive():
                    th.join()
                    th.terminate()
                    if self.bar is not None:
                        self.bar.update()
                else:
                    self.runQ.appendleft(th)
            except IndexError:
                pass

    def join(self):
        for th in self.runQ:
            th.join()
            th.terminate()
            if self.bar is not None:
                self.bar.update()

    def load_threads(self):
        # keep adding running thread until max thread reached
        while len(self.runQ) < self.max:
            try:
                arg = self.args.pop()
                p = Process(target=self.fn, args=(arg,))
                p.start()
                self.runQ.appendleft(p)
            except IndexError:
                break

    def start(self):
        while len(self.args) != 0:
            self.load_threads()
            self.terminate_dead()

        self.join()

    def run_pool(self):
        with Pool(processes=self.max) as pool:
            pool.map(self.fn, self.args_pure)


