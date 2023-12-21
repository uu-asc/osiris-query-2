from time import perf_counter
from string import Template

import pandas as pd


class DotDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    __dir__ = dict.keys


class Ts:
    @property
    def timestamp(self):
        return f"{self.now:%d-%m-%Y %H:%M}"

    @property
    def datum(self):
        return f"{self.now:%d-%m-%Y}"

    @property
    def ymd(self):
        return f"{self.now:%Y%m%d}"

    @property
    def daymonth(self):
        return f"{self.now:%d %B}"

    @property
    def now(self):
        return pd.Timestamp.today()


class Stopwatch:
    SPLIT = "[finished in ${time}]"
    TOTAL = """
+------------------------------------------------------------------+
    :: TOTAL RUN TIME :: ${time}
+==================================================================+
"""

    def __init__(self, will_print=True):
        self.will_print = will_print
        self.times = []
        self.click()

    def click(self):
        now = perf_counter()
        self.times.append(now)

    def split(self):
        self.click()
        time = self.times[-1] - self.times[-2]
        if self.will_print:
            print(Template(self.SPLIT).substitute(time=self.format_time(time)))
        return time

    def total(self):
        self.click()
        time = self.times[-1] - self.times[0]
        if self.will_print:
            print(Template(self.TOTAL).substitute(time=self.format_time(time)))
        return time

    @staticmethod
    def format_time(time):
        return f"{time:.2f}s"

    @property
    def start(self):
        return self.times[0]

    @property
    def durations(self):
        return [t - self.start for t in self.times]

    @property
    def splits(self):
        return [t2 - t1 for t1, t2 in zip(self.times, self.times[1:])]


TS = Ts()
