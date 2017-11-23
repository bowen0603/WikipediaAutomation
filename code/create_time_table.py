from __future__ import print_function

__author__ = 'bobo'

class Timer:

    def __init__(self):
        self.starting_time = 979697313
        self.ending_time = 1433343910
        self.time_period = 3600 * 24 * 7 # set a week for now


    def create_time_table(self):
        cur_time = self.starting_time
        idx = 0
        fout = open("data/time_indices.csv", "w")

        while True:
            if cur_time + self.time_period < self.ending_time:
                print("{},{},{}".format(cur_time, cur_time + self.time_period, idx), file=fout)
                idx += 1
                cur_time += self.time_period
            else:
                print("{},{},{}".format(cur_time, self.ending_time, idx), file=fout)
                break

    def create_time_table_aaron(self):
        starting = 20010201000000
        ending = 20160801000000
        step = 1 # change month gap

        if step == 3:
            starting_month = 2
        else:
            starting_month = 1

        dict_convert = {}
        list_dates = []

        for year in range(2001, 2017):
            for month in range(starting_month, 13, step):

                if year == 2016 and month > 8:
                    continue

                import time
                import datetime
                d = datetime.date(year, month, 1)

                unixtime = time.mktime(d.timetuple())
                if month < 10:
                    date = "{}0{}01000000".format(year, month)
                else:
                    date = "{}{}01000000".format(year, month)
                list_dates.append(date)
                dict_convert[date] = int(unixtime)

        idx = 0
        fout = open("data/time_index_{}month.json".format(step), "w")
        for i in range(len(list_dates)-1):
            obj = {"index": idx,
                   "starting_date": list_dates[i],
                   "ending_date": list_dates[i+1],
                   "starting_time": dict_convert[list_dates[i]],
                   "ending_time": dict_convert[list_dates[i+1]]}
            idx += 1
            from json import dumps
            print(dumps(obj), file=fout)


def main():
    timer = Timer()
    # timer.create_time_table()
    timer.create_time_table_aaron()

main()